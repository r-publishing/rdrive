/**
 * Copyright © 2019 kevinpollet <pollet.kevin@gmail.com>`
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE.md file.
 */
import fs from "fs";
import { URL } from "url";
import chalk from "chalk";
import Fuse from "fuse-native";
//import fuse from "node-fuse-bindings";
const messageFileURL = new URL("../assets/message.json", import.meta.url);
const fileBuff = fs.readFileSync(messageFileURL.pathname);
const { value } = JSON.parse(fileBuff.toString());
const ops = {
    readdir: function (path, cb) {
        if (path === '/')
            return cb(null, ['test']);
        return cb(Fuse.ENOENT);
    },
    /*
    getattr: function (path: string, cb: any) {
      if (path === '/') return cb(null, stat({ mode: 'dir', size: 4096 }))
      if (path === '/test') return cb(null, stat({ mode: 'file', size: 11 }))
      return cb(Fuse.ENOENT)
    },
    */
    open: function (path, flags, cb) {
        return cb(0, 42);
    },
    release: function (path, fd, cb) {
        return cb(0);
    },
    read: function (path, fd, buf, len, pos, cb) {
        var str = 'hello world'.slice(pos, pos + len);
        if (!str)
            return cb(0);
        buf.write(str);
        return cb(str.length);
    }
};
const fuse = new Fuse("./mnt", ops, { debug: true });
console.log(value, chalk.blue(process.argv[2] || "Pika")); // eslint-disable-line
