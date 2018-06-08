namespace cpp alsched
namespace java tetrisched

typedef i32 JobID
enum job_t {
    JOB_MPI     = 0,
    JOB_HDFS    = 1,
    JOB_GPU     = 2,
    JOB_WEB     = 3,
    JOB_AVAIL   = 4,
    JOB_NONE    = 5,
    JOB_UNKNOWN = 6,
    JOB_MAX
}

service TetrischedService {
    void AddJob(1:JobID jobId, 2:job_t jobType, 3:i32 k, 4:i32 priority, 5:double duration, 6:double slowDuration),
    void FreeResources(1:set<i32> machines),
}

service YARNTetrischedService {
    void AllocResources(1:JobID jobId, 2:set<i32> machines),
}
