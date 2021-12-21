/**
 * Copyright © 2019 kevinpollet <pollet.kevin@gmail.com>`
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE.md file.
 */

 import fs from "fs";
 import { URL } from "url";
 import chalk from "chalk";
 //import Fuse from "fuse-native";

 import os from 'os';
 import path from 'path';
 import Fuse from './fusen';
 //import fuse from "node-fuse-bindings";

 import { DID } from 'dids';
 import KeyResolver from 'key-did-resolver';
 import { getResolver as getRchainResolver, Resolver, parse } from "rchain-did-resolver";

 import * as rchainToolkit from 'rchain-toolkit';
 //const rchainToolkit = rchainToolkit_;
 
 
 const messageFileURL = new URL("../assets/message.json", import.meta.url);
 //const fuseFileURL = new URL("../assets/fuse.node", import.meta.url);
 const fileBuff = fs.readFileSync(messageFileURL.pathname);

 //const Fuse = require('bindings')("../assets/fuse.node");
 //const Fuse = require('bindings')("../assets/fuse.node");
 const { value } = JSON.parse(fileBuff.toString());

 const ops = {
    readdir: function (path: string, cb: any) {
      if (path === '/') return cb(null, ['test'])
      return cb(Fuse.ENOENT)
    },
    getattr: function (path: string, cb: any) {
      if (path === '/') {
        return process.nextTick(cb, 0, {
          mtime: new Date(),
          atime: new Date(),
          ctime: new Date(),
          nlink: 1,
          size: 100,
          mode: 16877,
          uid: process.getuid ? process.getuid() : 0,
          gid: process.getgid ? process.getgid() : 0
        })
      }
      if (path === '/test') {
        return process.nextTick(cb, 0, {
          mtime: new Date(),
          atime: new Date(),
          ctime: new Date(),
          nlink: 1,
          size: 12,
          mode: 33188,
          uid: process.getuid ? process.getuid() : 0,
          gid: process.getgid ? process.getgid() : 0
        })
      }
      return cb(Fuse.ENOENT)
    },
    open: function (path: string, flags: any, cb: any) {
      return cb(0, 42)
    },
    release: function (path: string, fd: any, cb: any) {
      return cb(0)
    },
    read: function (path: string, fd: any, buf: any, len: number, pos: number, cb: any) {
      var str = 'hello world'.slice(pos, pos + len)
      if (!str) return cb(0)
      buf.write(str)
      return cb(str.length)
    }
  }

  //console.info(Fuse);
  //Fuse.fuse_native_unmount('/tmp/test');

  const revAddr = rchainToolkit.utils.revAddressFromPublicKey("043d2800b8d261797f81a64c96ee80774c2e2b54990fe6740baae593927c0df5f1abb13d10799c691910b22a230252f1f2b5a71da992c456d82ca037ad7daac4f9");
  console.info(revAddr);

  
  const didTest = async () => {
    //const did = new DID({ resolver: { ...await getRchainResolver, ...KeyResolver.getResolver() } })
    //const res = await did.resolve("did:rchain:5tz91pxmj1tn83faq356x9ey17y59gijuk646fowp8bc5kaama7k78/publisher");
    
    const resolver = new Resolver({
      ...getRchainResolver(),
    })
    const res = await resolver.resolve("did:rchain:5tz91pxmj1tn83faq356x9ey17y59gijuk646fowp8bc5kaama7k78/publisher");

    console.info(res);
    return res;
  }

  didTest().then((res) => {
    console.info("Resolved DID");
  })

  const fuse = new Fuse("./rdrive", ops, { displayFolder: "RChain Drive", mkdir: true, debug: false });
  fuse.mount(function (err: any) {
    console.info("Mounted");
    setTimeout( () => {
      fuse.unmount( () => {
        console.info("Unmounteddd");
      });
    }, 30000)
  })
 
 console.log(value, chalk.blue(process.argv[2] || "Pika")); // eslint-disable-line