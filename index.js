const express = require('express');
const NodeCache = require('node-cache');
const winston = require('winston');
const expressWinston = require('express-winston');
const app = express();
const port = 3000;


// An empty Map to store job data
const jobData = new Map();

// Using a set to maintain concurrency control
const lockedJobs = new Set();

// Using cache for faster process
const cache = new NodeCache({ stdTTL: 30 });

// Middleware to parse JSON requests
app.use(express.json());


// Logger Setup
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.json(),
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'combined.log' }),
    ],
});

// Express-Winston logger for HTTP requests
app.use(expressWinston.logger({
    transports: [
        new winston.transports.Console(),
        new winston.transports.File({ filename: 'http.log' }),
    ],
    format: winston.format.combine(
        winston.format.colorize(),
        winston.format.json()
    ),
    meta: false,
    msg: 'HTTP {{req.method}} {{req.url}}',
    expressFormat: true,
    colorize: true,
}));

// A Function to validate job data
function validateJobInput(jobid, jobValue) {
    if (!jobid || !jobValue) {
        throw new Error('Invalid input data');
    }
}

// Function to add a job with concurrency control
async function addJob(jobid, jobValue) {
    validateJobInput(jobid, jobValue);
    if (jobData.has(jobid)) {
        throw new Error('Job ID already exists.');
    }
    else if (lockedJobs.has(jobid)) {
        throw new Error('Job ID is already being processed.');
    }
    // acquired a lock for current job
    lockedJobs.add(jobid);

    // Simulating an asynchronous operation
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // Release the lock
    lockedJobs.delete(jobid);

    jobData.set(jobid, jobValue);

    // clearing cache to ensure all jobs are updated
    cache.del('allJobs');
}

// Function to retrieve all jobs with caching
async function getAllJobs(jobValue) {
    const cachedJobs = cache.get('allJobs');
    if (cachedJobs && !jobValue) {
        return cachedJobs;
    }
    const jobs = [];
    for (const [jobid, _jobValue] of jobData.entries()) {
        if (!jobValue || _jobValue >= jobValue) {
            jobs.push({ JobID: jobid, JobValue: _jobValue });
        }
    }
    // caching the result 
    jobs.sort((a, b) => a.JobValue - b.JobValue)
    cache.set('allJobs', jobs, 10);

    return jobs;
}

// Function to remove a job with concurrency control
async function removeJob(jobid) {
    validateJobInput(jobid, 100);

    if (!jobData.has(jobid)) {
        throw new Error('Job ID not found.');
    }
    else if (lockedJobs.has(jobid)) {
        throw new Error('Job ID is already being processed.');
    }

    // acquire a lock for this job
    lockedJobs.add(jobid);

    // Simulating an asynchronous operation
    await new Promise((resolve) => setTimeout(resolve, 1000));

    // release the lock
    lockedJobs.delete(jobid);

    jobData.delete(jobid);

    cache.del('allJobs');
}

// An endpoint for adding a job
app.post('/add', async (req, res) => {
    try {
        const { jobValue, jobid } = req.query;
        await addJob(jobid, jobValue);
        logger.info(`Added job: JobID=${jobid}, JobValue=${jobValue}`);
        return res.status(200).json({ stat: 'ok' });
    } catch (error) {
        logger.error(`Error adding job [POST /add]: ${error.message}`);
        return res.status(400).json({ stat: 'error', message: error.message });
    }
});

// An endpoint for retrieving all jobs
app.get('/all', async (req, res) => {
    try {
        const { jobValue } = req.query;
        const jobs = await getAllJobs(jobValue);
        logger.info('Retrieved all jobs');
        return res.status(200).json({ data: jobs });
    } catch (error) {
        logger.error(`Error retrieving jobs [GET /all]: ${error.message}`);
        return res.status(400).json({ stat: 'error', message: error.message });
    }
});

// An endpoint for removing a job
app.post('/remove', async (req, res) => {
    try {
        const { jobid } = req.query;
        await removeJob(jobid);
        logger.info(`Removed job: JobID=${jobid}`);
        return res.status(200).json({ stat: 'ok' });
    } catch (error) {
        console.log(req.query);
        logger.error(`Error removing job [POST /remove]: ${error.message}`);
        return res.status(400).json({ stat: 'error', message: error.message });
    }
});

// Express Server
app.listen(port, () => {
    console.log(`Server is running on port ${port}`);
});