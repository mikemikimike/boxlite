/*
 * Copyright 2025 Daytona Platforms Inc.
 * Modified by BoxLite AI, 2025-2026
 * SPDX-License-Identifier: AGPL-3.0
 */

import { Injectable, Logger, OnApplicationShutdown } from '@nestjs/common'
import { InjectRepository } from '@nestjs/typeorm'
import { EntityManager, IsNull, LessThan, Not, Repository } from 'typeorm'
import { metrics } from '@opentelemetry/api'
import { BoxUsagePeriod } from '../entities/box-usage-period.entity'
import { OnEvent } from '@nestjs/event-emitter'
import { BoxStateUpdatedEvent } from '../../box/events/box-state-updated.event'
import { BoxDesiredStateUpdatedEvent } from '../../box/events/box-desired-state-updated.event'
import { BoxState } from '../../box/enums/box-state.enum'
import { BoxDesiredState } from '../../box/enums/box-desired-state.enum'
import { BoxEvents } from './../../box/constants/box-events.constants'
import { Cron, CronExpression } from '@nestjs/schedule'
import { RedisLockProvider } from '../../box/common/redis-lock.provider'
import { BOX_WARM_POOL_UNASSIGNED_ORGANIZATION } from '../../box/constants/box.constants'
import { BoxUsagePeriodArchive } from '../entities/box-usage-period-archive.entity'
import { TrackableJobExecutions } from '../../common/interfaces/trackable-job-executions'
import { TrackJobExecution } from '../../common/decorators/track-job-execution.decorator'
import { setTimeout as sleep } from 'timers/promises'
import { LogExecution } from '../../common/decorators/log-execution.decorator'
import { WithInstrumentation } from '../../common/decorators/otel.decorator'
import { BoxRepository } from '../../box/repositories/box.repository'
import { Box } from '../../box/entities/box.entity'
import { Runner } from '../../box/entities/runner.entity'
import {
  BOX_STATES_BILLING_DISK_ONLY,
  BOX_STATES_WITHOUT_OPEN_PERIOD,
  BOX_STATES_WITH_OPEN_PERIOD,
  UsagePeriodShape,
  expectedOpenPeriod,
  sameShape,
} from './expected-usage-period'

/** How many drifted boxes one reconcile pass repairs per runner shard. */
const RECONCILE_BATCH_SIZE = 100

// A box that changed hands moments ago may simply be waiting for its own event
// handler: waitForLock() blocks until the per-box lock frees, and that lock's TTL
// is 60s, so a handler can legitimately take a full minute to reach the ledger.
// Reconciling inside that window would race the handler and open a period it is
// about to open itself, colliding on the one-open-period index. Two lock TTLs
// gives it room; anything under 60s is provably too short.
const RECONCILE_GRACE_MS = 2 * 60 * 1000

const getDriftCounter = () =>
  metrics.getMeter('').createCounter('usage_period_drift_repaired', {
    description: 'Open usage periods brought back in step with the box they bill for',
  })

/** What one drifted box looks like, joined against its open period (if any). */
interface DriftCandidate {
  box_id: string
  box_state: BoxState
  box_cpu: number
  box_gpu: number
  box_mem: number
  box_disk: number
  box_org: string
  box_region: string
  period_id: string | null
}

@Injectable()
export class UsageService implements TrackableJobExecutions, OnApplicationShutdown {
  activeJobs = new Set<string>()
  private readonly logger = new Logger(UsageService.name)

  constructor(
    @InjectRepository(BoxUsagePeriod)
    private boxUsagePeriodRepository: Repository<BoxUsagePeriod>,
    private readonly redisLockProvider: RedisLockProvider,
    private readonly boxRepository: BoxRepository,
    @InjectRepository(Runner)
    private readonly runnerRepository: Repository<Runner>,
  ) {}

  async onApplicationShutdown() {
    //  wait for all active jobs to finish
    while (this.activeJobs.size > 0) {
      this.logger.log(`Waiting for ${this.activeJobs.size} active jobs to finish`)
      await sleep(1000)
    }
  }

  @OnEvent(BoxEvents.DESIRED_STATE_UPDATED)
  @TrackJobExecution()
  async handleBoxDesiredStateUpdate(event: BoxDesiredStateUpdatedEvent) {
    await this.waitForLock(event.box.id)

    try {
      switch (event.newDesiredState) {
        case BoxDesiredState.DESTROYED: {
          await this.closeUsagePeriod(event.box.id)
          break
        }
      }
    } finally {
      this.releaseLock(event.box.id).catch((error) => {
        this.logger.error(`Error releasing lock for box ${event.box.id}`, error)
      })
    }
  }

  @OnEvent(BoxEvents.STATE_UPDATED)
  @TrackJobExecution()
  async handleBoxStateUpdate(event: BoxStateUpdatedEvent) {
    await this.waitForLock(event.box.id)

    try {
      switch (event.newState) {
        case BoxState.STARTED: {
          await this.closeUsagePeriod(event.box.id)
          await this.openUsagePeriodFor(event.box, event.newState)
          break
        }
        // Billing stops charging compute the moment a stop is requested, while
        // quota keeps counting it (BOX_STATES_CONSUMING_COMPUTE includes
        // STOPPING) because the runner has not released cpu/memory yet. The two
        // answer different questions; do not "reconcile" them without a pricing
        // decision.
        case BoxState.STOPPING:
          await this.closeUsagePeriod(event.box.id)
          await this.openUsagePeriodFor(event.box, event.newState)
          break
        // Safeguards if STOPPING state is skipped
        case BoxState.STOPPED: {
          const cpuUsagePeriod = await this.boxUsagePeriodRepository.findOne({
            where: {
              boxId: event.box.id,
              endAt: IsNull(),
              cpu: Not(0),
            },
          })
          if (cpuUsagePeriod) {
            await this.closeUsagePeriod(event.box.id)
            await this.openUsagePeriodFor(event.box, event.newState)
          }
          break
        }
        case BoxState.ERROR:
        case BoxState.ARCHIVED:
        case BoxState.DESTROYING:
        case BoxState.DESTROYED: {
          await this.closeUsagePeriod(event.box.id)
          break
        }
      }
    } finally {
      this.releaseLock(event.box.id).catch((error) => {
        this.logger.error(`Error releasing lock for box ${event.box.id}`, error)
      })
    }
  }

  /**
   * Opens the period the box's current state calls for. A box that bills nothing
   * (terminal) or whose state is still in flight gets none — the caller has
   * already closed whatever was open.
   */
  private async openUsagePeriodFor(box: Box, state: BoxState) {
    // The event's newState is the authority on where the box landed; the entity
    // it carries is a snapshot, and a synthetic transition may have been built
    // with a state of its own (see the warm-pool claim in box.service.ts).
    const expected = expectedOpenPeriod({ ...box, state })
    if (expected === null) {
      return
    }
    await this.createUsagePeriod(box, expected)
  }

  private async createUsagePeriod(
    box: Pick<Box, 'id' | 'organizationId' | 'region'>,
    shape: UsagePeriodShape,
    entityManager?: EntityManager,
  ) {
    const usagePeriod = new BoxUsagePeriod()
    usagePeriod.boxId = box.id
    usagePeriod.startAt = new Date()
    usagePeriod.endAt = null
    usagePeriod.cpu = shape.cpu
    usagePeriod.gpu = shape.gpu
    usagePeriod.mem = shape.mem
    usagePeriod.disk = shape.disk
    usagePeriod.organizationId = box.organizationId
    usagePeriod.region = box.region

    await (entityManager ? entityManager.save(usagePeriod) : this.boxUsagePeriodRepository.save(usagePeriod))
  }

  private async closeUsagePeriod(boxId: string) {
    const lastUsagePeriod = await this.boxUsagePeriodRepository.findOne({
      where: {
        boxId,
        endAt: IsNull(),
      },
    })

    if (lastUsagePeriod) {
      lastUsagePeriod.endAt = new Date()
      await this.boxUsagePeriodRepository.save(lastUsagePeriod)
    }
  }

  @Cron(CronExpression.EVERY_MINUTE, { name: 'close-and-reopen-usage-periods' })
  @TrackJobExecution()
  @LogExecution('close-and-reopen-usage-periods')
  @WithInstrumentation()
  async closeAndReopenUsagePeriods() {
    if (!(await this.redisLockProvider.lock('close-and-reopen-usage-periods', 60))) {
      return
    }

    const usagePeriods = await this.boxUsagePeriodRepository.find({
      where: {
        endAt: IsNull(),
        // 1 day ago
        startAt: LessThan(new Date(Date.now() - 1000 * 60 * 60 * 24)),
        organizationId: Not(BOX_WARM_POOL_UNASSIGNED_ORGANIZATION),
      },
      order: {
        startAt: 'ASC',
      },
      take: 100,
    })

    for (const usagePeriod of usagePeriods) {
      if (!(await this.aquireLock(usagePeriod.boxId))) {
        continue
      }

      // validate that the usage period should remain active just in case
      try {
        const box = await this.boxRepository.findOne({
          where: {
            id: usagePeriod.boxId,
          },
        })

        await this.boxUsagePeriodRepository.manager.transaction(async (transactionalEntityManager) => {
          // Close usage period
          const closeTime = new Date()
          usagePeriod.endAt = closeTime
          await transactionalEntityManager.save(usagePeriod)

          // Roll over with the resources the box calls for *now*, not the ones
          // the closing period happened to carry. Copying the old figures kept
          // any drift alive forever: a period left charging no cpu for a running
          // box was re-copied every day, and a disk resize that landed while the
          // box was stopped never reached the ledger at all. A box that is gone
          // or terminal yields no shape and so is not reopened, which is what
          // stops a deleted box from accruing.
          const expected = expectedOpenPeriod(box)
          if (expected !== null) {
            const newUsagePeriod = BoxUsagePeriod.fromUsagePeriod(usagePeriod)
            newUsagePeriod.startAt = closeTime
            newUsagePeriod.endAt = null
            newUsagePeriod.cpu = expected.cpu
            newUsagePeriod.gpu = expected.gpu
            newUsagePeriod.mem = expected.mem
            newUsagePeriod.disk = expected.disk
            await transactionalEntityManager.save(newUsagePeriod)
          }
        })
      } catch (error) {
        this.logger.error(`Error closing and reopening usage period ${usagePeriod.boxId}`, error)
      } finally {
        await this.releaseLock(usagePeriod.boxId)
      }
    }

    await this.redisLockProvider.unlock('close-and-reopen-usage-periods')
  }

  /**
   * Brings open periods back in step with the boxes they bill for.
   *
   * The ledger is maintained by in-process events, which are fire-and-forget: a
   * handler that dies, throws, or loses its process leaves the box and its period
   * disagreeing, and nothing notices. The roll-over cannot: it scans the period
   * table, so a box with no period at all is invisible to it, and its one-day
   * cutoff lets a wrong period bill for a further day.
   *
   * This pass scans from the *box* side instead, which is why both are needed —
   * their blind spots are opposite. The roll-over is the only place that can see
   * a period whose box row was deleted outright; this one is the only place that
   * can see a box that never got a period.
   */
  @Cron(CronExpression.EVERY_5_MINUTES, { name: 'reconcile-usage-periods' })
  @TrackJobExecution()
  @LogExecution('reconcile-usage-periods')
  @WithInstrumentation()
  async reconcileUsagePeriods() {
    const lockKey = 'reconcile-usage-periods'
    if (!(await this.redisLockProvider.lock(lockKey, 300))) {
      return
    }

    try {
      const graceCutoff = new Date(Date.now() - RECONCILE_GRACE_MS)
      // Shard by runner so each scan rides a runnerId index rather than reading
      // the box table end to end. Measured on 20k boxes across 50 runners, the
      // planner takes box_runnerid_idx (bitmap index scan, ~400 rows rechecked,
      // ~1.7ms) — runnerId is selective enough on its own that it prefers that
      // over the composite box_runner_state_desired_idx.
      //
      // Every runner is scanned, not just the ready ones: a box stranded on an
      // unhealthy runner is the likeliest to have missed its event. The trailing
      // null shard is not optional — a box that
      // reached DESTROYED or ARCHIVED has had its runnerId cleared, and those are
      // exactly the boxes whose periods must be closed.
      const runners = await this.runnerRepository.find({ select: { id: true } })
      const shards: (string | null)[] = [...runners.map((runner) => runner.id), null]

      for (const shard of shards) {
        const candidates = await this.findDriftCandidates(shard, graceCutoff)
        for (const candidate of candidates) {
          await this.repairDrift(candidate)
        }
      }
    } finally {
      await this.redisLockProvider.unlock(lockKey)
    }
  }

  /**
   * Boxes whose open period disagrees with them, in one left join.
   *
   * The resource comparison has to be qualified by state. A bare `p.cpu <> b.cpu`
   * is permanently true for every stopped box — a stopped period *should* charge
   * no cpu — and those false positives would fill each page and starve the real
   * drift out of the batch forever.
   */
  private findDriftCandidates(runnerId: string | null, graceCutoff: Date): Promise<DriftCandidate[]> {
    return (
      this.boxRepository
        .createQueryBuilder('b')
        .leftJoin(BoxUsagePeriod, 'p', 'p."boxId" = b.id AND p."endAt" IS NULL')
        .select([
          'b.id AS box_id',
          'b.state AS box_state',
          'b.cpu AS box_cpu',
          'b.gpu AS box_gpu',
          'b.mem AS box_mem',
          'b.disk AS box_disk',
          'b."organizationId" AS box_org',
          'b.region AS box_region',
          'p.id AS period_id',
        ])
        .where(runnerId === null ? 'b."runnerId" IS NULL' : 'b."runnerId" = :runnerId', { runnerId })
        // Written as equality to match the partial indexes' own `WHERE "pending"
        // = false`, so the planner may pick one where the data makes it
        // worthwhile. A null pending predates the column default; both spellings
        // exclude it.
        .andWhere('b.pending = false')
        .andWhere('b."updatedAt" < :graceCutoff', { graceCutoff })
        .andWhere('b.state IN (:...trackedStates)', {
          trackedStates: [...BOX_STATES_WITH_OPEN_PERIOD, ...BOX_STATES_WITHOUT_OPEN_PERIOD],
        })
        .andWhere(
          `(
           (b.state IN (:...withStates) AND (
                p.id IS NULL
             OR p.disk <> b.disk
             -- the ledger stores organizationId as text, the box table as uuid
             OR p."organizationId" <> b."organizationId"::text
             OR p.region <> b.region
             OR (b.state = :started AND (p.cpu <> b.cpu OR p.mem <> b.mem OR p.gpu <> b.gpu))
             OR (b.state IN (:...diskOnlyStates) AND (p.cpu <> 0 OR p.mem <> 0 OR p.gpu <> 0))
           ))
        OR (b.state IN (:...withoutStates) AND p.id IS NOT NULL)
        )`,
          {
            withStates: BOX_STATES_WITH_OPEN_PERIOD,
            withoutStates: BOX_STATES_WITHOUT_OPEN_PERIOD,
            diskOnlyStates: BOX_STATES_BILLING_DISK_ONLY,
            started: BoxState.STARTED,
          },
        )
        .orderBy('b."updatedAt"', 'ASC')
        .limit(RECONCILE_BATCH_SIZE)
        .getRawMany<DriftCandidate>()
    )
  }

  /**
   * Re-derives the period from the box and writes only if it still disagrees.
   *
   * The SQL above is a wide filter; the authority on what a period should charge
   * is {@link expectedOpenPeriod}, re-applied here under the per-box lock so a
   * candidate the event handler fixed in the meantime is left alone. Corrections
   * start now and are never backdated: the window a box spent mis-billed cannot
   * be reconstructed (its updatedAt has moved on for unrelated reasons), and
   * guessing it would replace a known gap with an invented charge.
   */
  private async repairDrift(candidate: DriftCandidate): Promise<void> {
    if (!(await this.aquireLock(candidate.box_id))) {
      return
    }

    try {
      const box = await this.boxRepository.findOne({ where: { id: candidate.box_id } })
      const expected = expectedOpenPeriod(box)

      const open = await this.boxUsagePeriodRepository.findOne({
        where: { boxId: candidate.box_id, endAt: IsNull() },
      })

      if (expected === null) {
        if (!open) {
          return
        }
        await this.closeUsagePeriod(candidate.box_id)
        this.recordDrift('orphan', candidate.box_id)
        return
      }

      if (
        open &&
        sameShape(open, expected) &&
        open.organizationId === box.organizationId &&
        open.region === box.region
      ) {
        return
      }

      await this.boxUsagePeriodRepository.manager.transaction(async (transactionalEntityManager) => {
        if (open) {
          open.endAt = new Date()
          await transactionalEntityManager.save(open)
        }
        await this.createUsagePeriod(box, expected, transactionalEntityManager)
      })
      this.recordDrift(open ? 'stale_shape' : 'missing', candidate.box_id)
    } catch (error) {
      this.logger.error(`Error reconciling usage period for box ${candidate.box_id}`, error)
    } finally {
      await this.releaseLock(candidate.box_id)
    }
  }

  private recordDrift(kind: 'missing' | 'orphan' | 'stale_shape', boxId: string): void {
    getDriftCounter().add(1, { kind })
    this.logger.warn(`Repaired ${kind} usage period drift for box ${boxId}`)
  }

  @Cron(CronExpression.EVERY_MINUTE, { name: 'archive-usage-periods' })
  @TrackJobExecution()
  @LogExecution('archive-usage-periods')
  @WithInstrumentation()
  async archiveUsagePeriods() {
    const lockKey = 'archive-usage-periods'
    if (!(await this.redisLockProvider.lock(lockKey, 60))) {
      return
    }

    await this.boxUsagePeriodRepository.manager.transaction(async (transactionalEntityManager) => {
      const usagePeriods = await transactionalEntityManager.find(BoxUsagePeriod, {
        where: {
          endAt: Not(IsNull()),
        },
        order: {
          startAt: 'ASC',
        },
        take: 1000,
      })

      if (usagePeriods.length === 0) {
        return
      }

      this.logger.debug(`Found ${usagePeriods.length} usage periods to archive`)

      await transactionalEntityManager.delete(
        BoxUsagePeriod,
        usagePeriods.map((usagePeriod) => usagePeriod.id),
      )
      await transactionalEntityManager.save(usagePeriods.map(BoxUsagePeriodArchive.fromUsagePeriod))
    })

    await this.redisLockProvider.unlock(lockKey)
  }

  private async waitForLock(boxId: string) {
    while (!(await this.aquireLock(boxId))) {
      await new Promise((resolve) => setTimeout(resolve, 500))
    }
  }

  private async aquireLock(boxId: string): Promise<boolean> {
    return await this.redisLockProvider.lock(`usage-period-${boxId}`, 60)
  }

  private async releaseLock(boxId: string) {
    await this.redisLockProvider.unlock(`usage-period-${boxId}`)
  }
}
