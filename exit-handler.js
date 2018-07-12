'use strict';

/* eslint-disable no-console */

const Promise = require('bluebird');

module.exports = (fn, timeout = 20 * 1000) => {
    const timeoutPromise = () => new Promise((resolve) => {
        setTimeout(() => {
            console.error(`Failed to gracefully exit within ${timeout}ms`);
            resolve();
        }, timeout);
    });

    const exit = (signal) => {
        console.error(`caught ${signal}, exiting...`);
        if (!fn) return Promise.resolve();
        return Promise.race([fn(signal), timeoutPromise]).catch((error) => {
            console.error(error.stack ? error.stack : error.toString());
            process.exit(1);
        });
    };
    process.on('SIGINT', () => {
        exit('SIGINT').then(() => {
            process.exit();
        });
    });
    process.on('SIGTERM', () => {
        exit('SIGTERM').then(() => {
            process.exit();
        });
    });
};
