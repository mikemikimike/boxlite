/*
 * Copyright 2026 BoxLite AI
 * SPDX-License-Identifier: AGPL-3.0-only
 */

import { Job } from '../entities/job.entity'
import { JobStatus, JobType, ResourceType } from '../dto/job.dto'
import { JobService } from './job.service'

function makePendingJob(id: string): Job {
  return {
    id,
    runnerId: 'runner-1',
    resourceType: ResourceType.BOX,
    resourceId: `box-${id}`,
    type: JobType.CREATE_BOX,
    status: JobStatus.PENDING,
    startedAt: null,
    completedAt: null,
    createdAt: new Date('2026-07-30T04:00:00.000Z'),
    updatedAt: new Date('2026-07-30T04:00:00.000Z'),
  } as unknown as Job
}

/**
 * Records the predicates each claim runs and lets a test decide, per job id,
 * whether the conditional UPDATE matched a row.
 */
function makeService(pendingJobs: Job[], claimMatches: (id: string) => boolean) {
  const wheres: Array<Record<string, unknown>> = []
  let returningCalled = false

  const queryBuilder = {
    update: jest.fn(() => queryBuilder),
    set: jest.fn(() => queryBuilder),
    where: jest.fn((_clause: string, params: Record<string, unknown>) => {
      wheres.push(params)
      return queryBuilder
    }),
    andWhere: jest.fn((_clause: string, params: Record<string, unknown>) => {
      wheres.push(params)
      return queryBuilder
    }),
    returning: jest.fn(() => {
      returningCalled = true
      return queryBuilder
    }),
    execute: jest.fn(async () => {
      const id = wheres.find((params) => 'id' in params)?.id as string
      wheres.length = 0
      if (!claimMatches(id)) {
        return { affected: 0, raw: [] }
      }
      const claimed = pendingJobs.find((job) => job.id === id) as Job
      return {
        affected: 1,
        raw: [{ ...claimed, status: JobStatus.IN_PROGRESS, startedAt: new Date() }],
      }
    }),
  }

  const jobRepository = {
    find: jest.fn().mockResolvedValue(pendingJobs),
    createQueryBuilder: jest.fn(() => queryBuilder),
    // Mirrors Repository.create hydrating a RETURNING * row into an entity.
    create: jest.fn((row: Job) => row),
    save: jest.fn(),
  }

  const service = new JobService(jobRepository as any, {} as any, {} as any)
  return { service, jobRepository, queryBuilder, returningCalled: () => returningCalled }
}

// claimPendingJobs is private; poll routing is exercised elsewhere.
function claimPendingJobs(service: JobService, runnerId: string, limit: number) {
  return (service as any).claimPendingJobs(runnerId, limit) as Promise<Array<{ id: string; status: JobStatus }>>
}

describe('JobService.claimPendingJobs', () => {
  it('claims each job with a status predicate so a concurrent poll cannot claim it twice', async () => {
    const jobs = [makePendingJob('job-1')]
    const { service, queryBuilder } = makeService(jobs, () => true)

    const claimed = await claimPendingJobs(service, 'runner-1', 10)

    expect(claimed).toHaveLength(1)
    expect(claimed[0].status).toBe(JobStatus.IN_PROGRESS)

    // The claim must be a compare-and-swap. Without `status = PENDING` in the
    // WHERE clause, save() would happily overwrite a job another poll already
    // moved to IN_PROGRESS — TypeORM does not check the version column here.
    expect(queryBuilder.andWhere).toHaveBeenCalledWith(expect.stringContaining('status'), {
      pending: JobStatus.PENDING,
    })
    expect(queryBuilder.set).toHaveBeenCalledWith(expect.objectContaining({ status: JobStatus.IN_PROGRESS }))
  })

  it('skips a job another poll already claimed instead of handing it out again', async () => {
    const jobs = [makePendingJob('job-taken'), makePendingJob('job-free')]
    const { service } = makeService(jobs, (id) => id === 'job-free')

    const claimed = await claimPendingJobs(service, 'runner-1', 10)

    expect(claimed.map((job) => job.id)).toEqual(['job-free'])
  })

  it('propagates a database failure instead of reporting it as a lost race', async () => {
    const jobs = [makePendingJob('job-1')]
    const { service, queryBuilder } = makeService(jobs, () => true)
    queryBuilder.execute.mockRejectedValueOnce(new Error('deadlock detected'))

    await expect(claimPendingJobs(service, 'runner-1', 10)).rejects.toThrow('deadlock detected')
  })
})
