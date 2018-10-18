#! /usr/bin/env python3.3
"""Detecting 'p4 job' references in a Git commit  message."""

def extract_jobs(desc):
    """Scan the commit description looking for "Jobs:" and extracting the job
    identifiers following the field label. Returns None if no jobs found.
    """
    if not desc:
        return None
    lines = desc.splitlines()
    for i in range(0, len(lines)):
        line = lines[i].strip()
        if line.startswith("Jobs:"):
            jobs = []
            line = line[5:]
            if line:
                jobs.append(line.strip())
            for i in range(i + 1, len(lines)):
                line = lines[i].strip()
                if not line or ' ' in line or ':' in line:
                    # reached the end of the job identifiers
                    break
                # whatever is left of the line is a job identifier
                jobs.append(line)
            return jobs
    return None


def lookup_jobs(ctx, job_list):
    """Adapter hook to permit job IDs such as "GF-1234" in addition
    to actual 'p4 job' ids like "job567890".
    """
    if not (job_list and ctx.job_lookup_list):
        return job_list

    return [lookup_job(ctx, j, ctx.job_lookup_list)
            for j in job_list]


def lookup_job(ctx, job_id, job_lookup_list):
    """Adapter hook to permit job IDs such as "GF-1234" in addition
    to actual 'p4 job' ids like "job567890".
    """
    if job_id.startswith("job"):
        return job_id

    for jq in job_lookup_list:
        r = ctx.p4run('jobs', '-e', jq.format(jobval=job_id))
        if len(r) == 1:
            try:
                return r[0]["Job"]
            except KeyError:
                pass
            except TypeError:
                pass
    return job_id
