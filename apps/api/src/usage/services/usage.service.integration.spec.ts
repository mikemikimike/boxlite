/*
 * Copyright 2025 BoxLite AI
 * SPDX-License-Identifier: AGPL-3.0
 */

import { Redis } from 'ioredis'
import { DataSource, IsNull, Repository } from 'typeorm'
import { Box } from '../../box/entities/box.entity'
import { BoxLastActivity } from '../../box/entities/box-last-activity.entity'
import { BoxState } from '../../box/enums/box-state.enum'
import { BOX_WARM_POOL_UNASSIGNED_ORGANIZATION } from '../../box/constants/box.constants'
import { RedisLockProvider } from '../../box/common/redis-lock.provider'
import { CustomNamingStrategy } from '../../common/utils/naming-strategy.util'
import { Migration1741087887225 } from '../../migrations/1741087887225-migration'
import { AddBoxLifecycleSeconds1784250000000 } from '../../migrations/pre-deploy/1784250000000-add-box-lifecycle-seconds-migration'
import { AddBoxUsagePeriods1785250000000 } from '../../migrations/pre-deploy/1785250000000-add-box-usage-periods-migration'
import { BoxUsagePeriod } from '../entities/box-usage-period.entity'
import { BoxUsagePeriodArchive } from '../entities/box-usage-period-archive.entity'
import { UsageService } from './usage.service'
import { expectedOpenPeriod } from './expected-usage-period'

// The three cron jobs are a destructive archive transaction, a roll-over that
// rewrites resources, and a reconcile pass built on a left join from box to the
// ledger — none is observable without a real database, and the
// at-most-one-open-period invariant lives in a Postgres partial index whose
// WHERE clause no schema differ compares. The schema here is built by running
// the migrations, so these tests exercise the DDL that actually ships. Runs only
// when a Postgres and a Redis are reachable; skipped otherwise.
const describeIfDatabase = process.env.DB_HOST && process.env.REDIS_HOST ? describe : describe.skip

const DAY_MS = 24 * 60 * 60 * 1000
// Cleared between tests. `box` is here because the reconcile pass reads it; the
// rest of the schema the initial migration builds is left in place untouched.
const TABLES = ['box_usage_periods', 'box_usage_periods_archive', 'box']
// Older than the reconcile pass's grace window, so a box is eligible unless a
// test deliberately makes it fresh.
const SETTLED_AGO_MS = 10 * 60 * 1000

describeIfDatabase('UsageService (integration, real Postgres + Redis)', () => {
  let dataSource: DataSource
  let redis: Redis
  let periods: Repository<BoxUsagePeriod>
  let archives: Repository<BoxUsagePeriodArchive>
  // Set only once this spec has built the tables itself. Until then the rows in
  // them belong to somebody else and nothing here may write or clear them.
  let ownsTables = false

  // organizationId is a uuid on the box table, so it has to be a real one here
  // even though the ledger stores it as text.
  const box = {
    id: 'box-int-1',
    organizationId: '5c2f6a48-8f2b-4a1e-9d31-2b7e6f0c1a45',
    region: 'us',
    cpu: 2,
    gpu: 1,
    mem: 4,
    disk: 10,
  }

  // The service's own lock keys — cleared individually so a parallel spec
  // sharing this Redis keeps its state.
  const lockKeys = [
    `usage-period-${box.id}`,
    'close-and-reopen-usage-periods',
    'archive-usage-periods',
    'reconcile-usage-periods',
  ]

  const openPeriod = (overrides: Partial<BoxUsagePeriod> = {}) =>
    periods.save(
      periods.create({
        boxId: box.id,
        organizationId: box.organizationId,
        region: box.region,
        cpu: box.cpu,
        gpu: box.gpu,
        mem: box.mem,
        disk: box.disk,
        startAt: new Date(),
        endAt: null,
        ...overrides,
      }),
    )

  const serviceForBoxState = (state: BoxState, boxOverrides: Partial<typeof box> = {}) =>
    new UsageService(
      periods,
      new RedisLockProvider(redis),
      { findOne: async () => ({ ...box, state, ...boxOverrides }) } as any,
      { find: async () => [] } as any,
    )

  const openPeriods = () => periods.find({ where: { endAt: IsNull() } })

  /**
   * Inserts a real box row. The reconcile pass joins the box table, so these
   * cases cannot use the stub above.
   */
  const insertBox = (
    overrides: Partial<typeof box> & { state: BoxState; runnerId?: string; pending?: boolean; updatedAtMsAgo?: number },
  ) => {
    const row = {
      ...box,
      runnerId: null as string | null,
      pending: false,
      updatedAtMsAgo: SETTLED_AGO_MS,
      ...overrides,
    }
    return dataSource.query(
      `INSERT INTO "box" ("id","organizationId","name","region","osUser","authToken","state","desiredState",
                          "cpu","gpu","mem","disk","runnerId","pending","updatedAt")
       VALUES ($1,$2,$3,$4,'root',$5,$6,'started',$7,$8,$9,$10,$11,$12,$13)`,
      [
        row.id,
        row.organizationId,
        `name-${row.id}`,
        row.region,
        `token-${row.id}`,
        row.state,
        row.cpu,
        row.gpu,
        row.mem,
        row.disk,
        row.runnerId,
        row.pending,
        new Date(Date.now() - row.updatedAtMsAgo),
      ],
    )
  }

  /**
   * Everything the control plane runs to keep the ledger in step with box state:
   * the roll-over settles periods that have grown too long, the reconcile pass
   * settles periods that disagree with the box they bill for. Reads boxes from
   * the real table — a plain TypeORM repository satisfies the two methods the
   * service uses (findOne and createQueryBuilder).
   *
   * @param runnerIds shards the reconcile pass will visit on top of the always
   *   scanned "no runner" shard.
   */
  const settleLedger = async (runnerIds: string[] = []) => {
    const service = new UsageService(
      periods,
      new RedisLockProvider(redis),
      dataSource.getRepository(Box) as any,
      { find: async () => runnerIds.map((id) => ({ id })) } as any,
    )
    await service.closeAndReopenUsagePeriods()
    await service.reconcileUsagePeriods()
  }

  const quoted = TABLES.map((table) => `"${table}"`).join(', ')
  // CASCADE because box_last_activity references box.
  const truncateTables = () => dataSource.query(`TRUNCATE ${quoted} CASCADE`)

  // This spec rebuilds the entire public schema from the migrations, so it must
  // never be pointed at a database holding real data. Every table is checked,
  // not just the ledger's: one populated table anywhere is enough to refuse.
  const assertDisposableDatabase = async () => {
    const tables: { table_name: string }[] = await dataSource.query(
      `SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'`,
    )

    for (const { table_name } of tables) {
      const [{ rows }] = await dataSource.query(`SELECT count(*)::int AS rows FROM "${table_name}"`)
      if (rows > 0) {
        throw new Error(
          `refusing to run: "${table_name}" in database "${process.env.DB_DATABASE}" already holds rows — point DB_* at a disposable database`,
        )
      }
    }
  }

  beforeAll(async () => {
    dataSource = await new DataSource({
      type: 'postgres',
      host: process.env.DB_HOST,
      port: Number(process.env.DB_PORT || 5432),
      username: process.env.DB_USERNAME,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_DATABASE,
      entities: [BoxUsagePeriod, BoxUsagePeriodArchive, Box, BoxLastActivity],
      namingStrategy: new CustomNamingStrategy(),
      synchronize: false,
    }).initialize()

    // Rebuild the schema from the migrations every run, so these tests exercise
    // the DDL that ships rather than whatever an earlier run happened to leave.
    // The whole schema, not just the ledger: the reconcile pass joins the box
    // table, and box only exists as part of the initial migration.
    await assertDisposableDatabase()
    await dataSource.query(`DROP SCHEMA public CASCADE`)
    await dataSource.query(`CREATE SCHEMA public`)
    await dataSource.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`)
    const queryRunner = dataSource.createQueryRunner()
    try {
      await new Migration1741087887225().up(queryRunner)
      await new AddBoxLifecycleSeconds1784250000000().up(queryRunner)
      await new AddBoxUsagePeriods1785250000000().up(queryRunner)
      ownsTables = true
    } finally {
      await queryRunner.release()
    }

    redis = new Redis({
      host: process.env.REDIS_HOST,
      port: Number(process.env.REDIS_PORT || 6379),
      maxRetriesPerRequest: 2,
    })
    periods = dataSource.getRepository(BoxUsagePeriod)
    archives = dataSource.getRepository(BoxUsagePeriodArchive)
  })

  // Setup can throw (the disposable-database guard), so nothing here may assume
  // it ran to completion.
  afterAll(async () => {
    if (redis) {
      await redis.del(...lockKeys)
      await redis.quit()
    }
    if (dataSource?.isInitialized) {
      if (ownsTables) {
        // leave the schema in place but carrying nothing, so a later run finds
        // the database exactly as disposable as it expects
        await truncateTables().catch(() => undefined)
      }
      await dataSource.destroy()
    }
  })

  beforeEach(async () => {
    await truncateTables()
    await redis.del(...lockKeys)
  })

  it('archives closed periods and leaves the open one in place', async () => {
    await openPeriod({ startAt: new Date(Date.now() - 2 * DAY_MS), endAt: new Date(Date.now() - DAY_MS) })
    const stillOpen = await openPeriod()

    await serviceForBoxState(BoxState.STARTED).archiveUsagePeriods()

    expect(await periods.find()).toEqual([expect.objectContaining({ id: stillOpen.id })])
    expect(await archives.find()).toEqual([
      expect.objectContaining({ boxId: box.id, organizationId: box.organizationId, cpu: box.cpu }),
    ])
  })

  it('rolls a day-old period over, carrying the resources of a running box', async () => {
    await openPeriod({ startAt: new Date(Date.now() - DAY_MS - 60_000) })

    await serviceForBoxState(BoxState.STARTED).closeAndReopenUsagePeriods()

    const [closed, reopened] = await periods.find({ order: { startAt: 'ASC' } })
    expect(closed.endAt).toBeInstanceOf(Date)
    expect(reopened).toEqual(expect.objectContaining({ endAt: null, cpu: 2, gpu: 1, mem: 4, disk: 10 }))
  })

  it('stops charging compute when it rolls over a period whose box is already stopped', async () => {
    // a box that reached STOPPED without passing through STOPPING keeps a
    // full-resource period open; the roll-over must not re-bill its cpu forever
    await openPeriod({ startAt: new Date(Date.now() - DAY_MS - 60_000) })

    await serviceForBoxState(BoxState.STOPPED).closeAndReopenUsagePeriods()

    const [, reopened] = await periods.find({ order: { startAt: 'ASC' } })
    expect(reopened).toEqual(expect.objectContaining({ endAt: null, cpu: 0, gpu: 0, mem: 0, disk: 10 }))
  })

  it('leaves a period younger than a day alone', async () => {
    const fresh = await openPeriod({ startAt: new Date(Date.now() - 60 * 60 * 1000) })

    await serviceForBoxState(BoxState.STARTED).closeAndReopenUsagePeriods()

    expect(await periods.find()).toEqual([expect.objectContaining({ id: fresh.id, endAt: null })])
  })

  it('leaves warm-pool periods alone, since no organization owns them yet', async () => {
    const warmPool = await openPeriod({
      organizationId: BOX_WARM_POOL_UNASSIGNED_ORGANIZATION,
      startAt: new Date(Date.now() - DAY_MS - 60_000),
    })

    await serviceForBoxState(BoxState.STARTED).closeAndReopenUsagePeriods()

    expect(await periods.find()).toEqual([expect.objectContaining({ id: warmPool.id, endAt: null })])
  })

  it('starts the reopened period exactly where the closed one ended, with the same attribution', async () => {
    await openPeriod({ startAt: new Date(Date.now() - DAY_MS - 60_000) })

    await serviceForBoxState(BoxState.STOPPING).closeAndReopenUsagePeriods()

    const [closed, reopened] = await periods.find({ order: { startAt: 'ASC' } })
    // no gap and no overlap: an inherited startAt would bill the elapsed day twice
    expect(reopened.startAt).toEqual(closed.endAt)
    expect(reopened).toEqual(
      expect.objectContaining({ organizationId: box.organizationId, region: box.region, endAt: null }),
    )
  })

  it('closes without reopening when the box row no longer exists', async () => {
    await openPeriod({ startAt: new Date(Date.now() - DAY_MS - 60_000) })
    const serviceWithoutBox = new UsageService(
      periods,
      new RedisLockProvider(redis),
      { findOne: async () => null } as any,
      { find: async () => [] } as any,
    )

    await serviceWithoutBox.closeAndReopenUsagePeriods()

    // a missing box must not throw inside the transaction — that would roll the
    // close back and leave the period accruing forever
    const remaining = await periods.find()
    expect(remaining).toHaveLength(1)
    expect(remaining[0].endAt).toBeInstanceOf(Date)
  })

  it('does not reopen a period for a box that is already gone', async () => {
    await openPeriod({ startAt: new Date(Date.now() - DAY_MS - 60_000) })

    await serviceForBoxState(BoxState.DESTROYED).closeAndReopenUsagePeriods()

    const remaining = await periods.find()
    expect(remaining).toHaveLength(1)
    expect(remaining[0].endAt).toBeInstanceOf(Date)
  })

  // The ledger is maintained by in-process events, which are fire-and-forget: a
  // dropped one leaves the open period disagreeing with the box it bills for.
  // These four assert the invariant the control plane owes its tenants — the
  // open period matches the box's current state — after everything it runs to
  // settle the ledger has run. Nothing here reaches for an unwritten method;
  // each drives the shipped roll-over and states what the ledger should hold.
  describe('box state versus the open period', () => {
    it('opens a period for a running box that has none', async () => {
      // the STARTED event never reached the handler, so nothing was ever opened
      await insertBox({ state: BoxState.STARTED })

      await settleLedger()

      const open = await openPeriods()
      expect(open).toHaveLength(1)
      expect(open[0]).toEqual(
        expect.objectContaining({
          boxId: box.id,
          organizationId: box.organizationId,
          region: box.region,
          cpu: box.cpu,
          gpu: box.gpu,
          mem: box.mem,
          disk: box.disk,
        }),
      )
    })

    it('restores compute billing for a running box whose period charges no cpu', async () => {
      // the box was started again but the STARTED event was lost, so the period
      // left over from the stop is still open and still charging nothing
      await insertBox({ state: BoxState.STARTED })
      await openPeriod({ cpu: 0, gpu: 0, mem: 0 })

      await settleLedger()

      expect(await periods.findOne({ where: { endAt: IsNull() } })).toEqual(
        expect.objectContaining({ cpu: box.cpu, gpu: box.gpu, mem: box.mem }),
      )
    })

    it('follows a disk resize that landed while the box was stopped', async () => {
      // RESIZING -> STOPPED changed the box's disk; the stopped period's cpu is
      // already 0, so the STOPPED branch's safeguard never fires
      await insertBox({ state: BoxState.STOPPED, disk: 100 })
      await openPeriod({ cpu: 0, gpu: 0, mem: 0, disk: 10 })

      await settleLedger()

      expect(await periods.findOne({ where: { endAt: IsNull() } })).toEqual(expect.objectContaining({ disk: 100 }))
    })

    it('closes the open period of a box that already reached a terminal state', async () => {
      // destroyed an hour ago: the box stopped consuming anything, but the
      // period is younger than the roll-over's one-day cutoff
      await insertBox({ state: BoxState.DESTROYED })
      await openPeriod({ startAt: new Date(Date.now() - 60 * 60 * 1000) })

      await settleLedger()

      expect(await openPeriods()).toHaveLength(0)
    })

    it('reaches boxes assigned to a runner, not just unassigned ones', async () => {
      const runnerId = 'b41f1d0e-9c3a-4f77-8a2d-6e5b3c1d0af2'
      await insertBox({ state: BoxState.STARTED, runnerId })

      await settleLedger([runnerId])

      expect(await openPeriods()).toHaveLength(1)
    })

    it('leaves a box alone until its own event handler has had time to run', async () => {
      // inside the grace window the handler may still be waiting on the per-box
      // lock; opening a period here would race it and collide on the index
      await insertBox({ state: BoxState.STARTED, updatedAtMsAgo: 0 })

      await settleLedger()

      expect(await openPeriods()).toHaveLength(0)
    })

    it('leaves a box mid-transition alone', async () => {
      await insertBox({ state: BoxState.STARTING })

      await settleLedger()

      expect(await openPeriods()).toHaveLength(0)
    })

    it('keeps the period a box already holds while it is mid-transition', async () => {
      // A box on its way up from STOPPED still holds the disk-only period the
      // stop opened. expectedOpenPeriod answers null for STARTING exactly as it
      // does for a terminal box, so the only thing standing between that period
      // and a close is that STARTING is in neither of the two state lists the
      // drift predicate matches on. Adding it to BOX_STATES_WITHOUT_OPEN_PERIOD
      // stops billing disk for every box for the length of its startup.
      await insertBox({ state: BoxState.STARTING })
      const held = await openPeriod({ cpu: 0, gpu: 0, mem: 0 })

      await settleLedger()

      expect(await periods.find()).toEqual([expect.objectContaining({ id: held.id, endAt: null })])
    })

    it('leaves a box alone while a state change is in flight', async () => {
      await insertBox({ state: BoxState.STARTED, pending: true })

      await settleLedger()

      expect(await openPeriods()).toHaveLength(0)
    })

    it('does not touch a period that already agrees with its box', async () => {
      await insertBox({ state: BoxState.STARTED })
      const settled = await openPeriod()

      await settleLedger()
      await settleLedger()

      // a rewrite on every pass would shred the ledger into unbillable slivers
      expect(await periods.find()).toEqual([expect.objectContaining({ id: settled.id, endAt: null })])
    })

    // The reconcile pass filters candidates in SQL while the repair re-derives
    // the shape in TypeScript, so the state -> period rule exists twice. This
    // pins the two together: any state where they disagree is a rule that was
    // changed on one side only.
    it.each(Object.values(BoxState))('agrees with expectedOpenPeriod about %s', async (state) => {
      await insertBox({ state })
      const expected = expectedOpenPeriod({ ...box, state })

      await settleLedger()

      const open = await openPeriods()
      if (expected === null) {
        expect(open).toHaveLength(0)
        return
      }
      expect(open).toHaveLength(1)
      expect(open[0]).toEqual(expect.objectContaining(expected))
    })
  })

  // The roll-over runs on its own schedule and must leave a correct ledger by
  // itself. Driving it alone here matters: with the reconcile pass also running,
  // it would paper over a roll-over that carried the wrong figures forward, and
  // the ledger would still churn out a wrong period every single day.
  describe('the daily roll-over on its own', () => {
    const rollOver = (state: BoxState, boxOverrides: Partial<typeof box> = {}) =>
      serviceForBoxState(state, boxOverrides).closeAndReopenUsagePeriods()

    it('carries the resources the box has now, not the ones the closing period held', async () => {
      await openPeriod({ cpu: 0, gpu: 0, mem: 0, startAt: new Date(Date.now() - DAY_MS - 60_000) })

      await rollOver(BoxState.STARTED)

      expect(await periods.findOne({ where: { endAt: IsNull() } })).toEqual(
        expect.objectContaining({ cpu: box.cpu, gpu: box.gpu, mem: box.mem }),
      )
    })

    it('picks up a disk resize the ledger never heard about', async () => {
      await openPeriod({ cpu: 0, gpu: 0, mem: 0, disk: 10, startAt: new Date(Date.now() - DAY_MS - 60_000) })

      await rollOver(BoxState.STOPPED, { disk: 100 })

      expect(await periods.findOne({ where: { endAt: IsNull() } })).toEqual(expect.objectContaining({ disk: 100 }))
    })

    it('stops charging compute for a stopping box whose period still bills it', async () => {
      // STOPPING bills disk only, exactly like STOPPED — a period that still
      // charges cpu here has to be cut over, not copied forward
      await openPeriod({ startAt: new Date(Date.now() - DAY_MS - 60_000) })

      await rollOver(BoxState.STOPPING)

      expect(await periods.findOne({ where: { endAt: IsNull() } })).toEqual(
        expect.objectContaining({ cpu: 0, gpu: 0, mem: 0, disk: box.disk }),
      )
    })

    it('does not reopen a period for a state this repository never assigns', async () => {
      // ARCHIVING has no writer anywhere in the codebase, so it carries no
      // price and the roll-over must behave exactly as it did before: close the
      // over-long period and leave it closed, rather than invent a charge.
      await openPeriod({ cpu: 0, gpu: 0, mem: 0, startAt: new Date(Date.now() - DAY_MS - 60_000) })

      await rollOver(BoxState.ARCHIVING)

      expect(await openPeriods()).toHaveLength(0)
    })
  })

  it('declares the open-period invariant on the entity as well as in the migration', () => {
    const index = dataSource
      .getMetadata(BoxUsagePeriod)
      .indices.find((candidate) => candidate.name === 'box_usage_periods_one_open_period_per_box_idx')

    expect(index).toMatchObject({ isUnique: true, where: '"endAt" IS NULL' })
    expect(index?.columns.map((column) => column.propertyName)).toEqual(['boxId'])
  })

  it('refuses a second open period for the same box', async () => {
    await openPeriod()

    await expect(openPeriod()).rejects.toThrow(/box_usage_periods_one_open_period_per_box_idx/)
  })
})
