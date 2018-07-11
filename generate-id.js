'use strict';

const nanoid = require('nanoid/generate');

module.exports = (prefix, length = 5) => {
    const characters = '-0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
    const id = nanoid(characters, length).replace(/^-/, 's');
    if (prefix) {
        return `${prefix}-${id}`;
    }
    return id;
};
