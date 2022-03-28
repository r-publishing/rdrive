/**
 * Copyright © 2019 kevinpollet <pollet.kevin@gmail.com>`
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE.md file.
 */

import fs from "fs";
import { URL } from "url";
import process from "process";

import path from 'path';
import Fuse from './fusen';


import * as rchainToolkit from 'rchain-toolkit';

import { createRequire } from 'module';

import {
  FuseCallback,
  setPathNode,
  mkDir,
  mkFd,
  getPathNode,
  Node,
  getFsStats,
  isRegularFile,
  isDirectory,
  mkFile,
  truncateFile,
  addNodeToParent,
  updateNodeContent,
  changeNodeOwner,
  getPathFromName,
  getNameFromPath,
  removeNodeFromParent
} from "./cache";

import { sep as pathSep } from 'path';
import { constants } from 'fs';

const require = createRequire(import.meta.url);

const {
  readPursesTerm,
  readBoxTerm,
  readPursesDataTerm,
  decodePurses,
  updatePurseDataTerm,
  readBoxesTerm,
  deployBoxTerm,
  createPursesTerm,
  deletePurseTerm,
  updatePursePriceTerm,
  withdrawTerm,
  creditTerm,
  swapTerm
} = require("rchain-token");

const service = require("os-service");
const commandLineArgs = require('command-line-args')
 
 
const message_file_path = path.join(import.meta.url, '../assets/message.json');
const fuse_node_path = path.join(import.meta.url, '../assets/fuse.node');
const libfuse_path = path.join(import.meta.url, '../assets/libfuse.so');


//const messageFileURL = new URL("../assets/message.json", import.meta.url);
//const fileBuff = fs.readFileSync(messageFileURL.pathname);
//const { value } = JSON.parse(fileBuff.toString());


const QueuedJobs = require("queued-jobs").default;
const readQueue = new QueuedJobs();
const writeQueue = new QueuedJobs();

const readBundleQueue = new QueuedJobs();
const writeBundleQueue = new QueuedJobs();

const pursesToRead = new Set();

type QueueType = {
  id: string;
  tag: number;
  payload: any;
  priority: number;
  revert: () => void;
  success: () => void;
};

const deployQueue = new Map<number, Array<QueueType>>();
const exploreDeployQueue = new Map<number, Array<QueueType>>();

readQueue.registerHandler(async (data: QueueType) => {
  const termGenerator = exploreDeployTermGenerators.get(data.tag);
  const term = termGenerator(data.payload);
  const result = await rchainToolkit.http.exploreDeploy(READ_ONLY_HOST, {
    term: term
  });
  return result;
});

readBundleQueue.registerHandler(async (data: QueueType) => {
  if (!pursesToRead.has(data)) {
    pursesToRead.add(data);
  }
  return data;
});

var logStream = fs.createWriteStream(process.argv[0] + ".log");

const VALIDATOR_HOST = "http://127.0.0.1:40403";
const READ_ONLY_HOST = "http://127.0.0.1:40403";
const numberOfValidators = 1;
const contractName = "mynft";


writeQueue.registerHandler(async (data: QueueType) => {
  /*
  const termGenerator = deployTermGenerators.get(data.tag);
  const term = termGenerator(data.payload);
  rchainToolkit.http.easyDeploy(
    VALIDATOR_HOST,
    term,
    privateKey,
    1,
    10000000,
    10 * 60 * 1000
  );
  */
  if (!deployQueue.has(data.tag)) {
    deployQueue.set(data.tag, [data]);
  } else {
    deployQueue.get(data.tag).push(data);
  }
})


const deployBundler = async function() {
  const promises = [];
  const reverts = new Map<number, Array<() => any>>();
  const successMap = new Map<number, Array<() => any>>();
  const deploys = new Map<string, QueueType>();
  console.info("tags: ", deployQueue.size);
  let findTagById = Array<number>();
  for (const [tag, queue] of deployQueue) {
    findTagById.push(tag);
    if (!reverts.has(tag)) {
      reverts.set(tag, []);
    }
    if (!successMap.has(tag)) {
      successMap.set(tag, []);
    }
    const queueLength = queue.length;

    switch (tag) {
      //Here we bundle all CREATE_PURSES deploys into one + any other deploys like UPDATE_PURSE_DATA.
      case Deploy.CREATE_PURSES:
        let payload = {
          masterRegistryUri,
          contractId: contractName,
          purses: {},
          data: {}
        };

        for (var i = queueLength - 1; i >= 0; i--) {
          const data = queue.at(i);
          
          payload.purses = {...payload.purses, ...data.payload.purses};
          payload.data = {...payload.data, ...data.payload.data};
          reverts.get(tag).push(data.revert);
          successMap.get(tag).push(data.success);
          deploys.set(tag.toString() + data.id, data);
        }

        if (deployQueue.has(Deploy.UPDATE_PURSE_DATA)) {
          const dataQueue = deployQueue.get(Deploy.UPDATE_PURSE_DATA);
          for (var i = dataQueue.length - 1; i >= 0; i--) {
            const dataDeploy = dataQueue.at(i);

            if (!Object.keys(payload.purses).includes(dataDeploy.id)) {
              continue;
            }

            payload.data[dataDeploy.id] = dataDeploy.payload.data;
            
            if (!reverts.has(Deploy.UPDATE_PURSE_DATA)) {
              reverts.set(Deploy.UPDATE_PURSE_DATA, [dataDeploy.revert]);
            } else {
              reverts.get(Deploy.UPDATE_PURSE_DATA).push(dataDeploy.revert);
            }
            deploys.set(Deploy.UPDATE_PURSE_DATA.toString() + dataDeploy.id, dataDeploy);
            deployQueue.set(Deploy.UPDATE_PURSE_DATA, dataQueue.filter( el => el.id !== dataDeploy.id));
          }
        }
        /*
        if (deployQueue.has(Deploy.UPDATE_PURSE_PRICE)) {
          const priceQueue = deployQueue.get(Deploy.UPDATE_PURSE_PRICE);

          for (var i = priceQueue.length - 1; i >= 0; i--) {
            const priceDeploy = priceQueue.at(i);

            if (!Object.keys(payload.purses).includes(priceDeploy.id)) {
              continue;
            }

            const price = JSON.parse("[" + priceDeploy.payload.price + "]");
            payload.purses[priceDeploy.id].price = [price[0] , price[1]];

            if (!reverts.has(Deploy.UPDATE_PURSE_PRICE)) {
              reverts.set(Deploy.UPDATE_PURSE_PRICE, [priceDeploy.revert]);
            } else {
              reverts.get(Deploy.UPDATE_PURSE_PRICE).push(priceDeploy.revert);
            }
            deploys.set(Deploy.UPDATE_PURSE_PRICE.toString() + priceDeploy.id, priceDeploy);
            deployQueue.set(Deploy.UPDATE_PURSE_PRICE, priceQueue.filter( el => el.id !== priceDeploy.id));
          }
        }
        */

        try {
          const termGenerator = deployTermGenerators.get(tag);
          console.info("currently deploying:");
          console.info(payload);
          const term = termGenerator(payload);
          console.info(term);
          promises.push(rchainToolkit.http.easyDeploy(
            VALIDATOR_HOST,
            term,
            privateKey,
            1,
            10000000,
            10 * 60 * 1000
          ));
          deployQueue.delete(tag);
        }
        catch (err) {
          console.info(err);
          //TODO: retry?
        }

        return;
      default:
        for (var i = queueLength - 1; i >= 0; i--) {
          const data = queue.pop();
          if (deploys.has(Deploy.UPDATE_PURSE_DATA.toString() + data.id)) {
            //Already processed
            continue;
          }
          /*
          if (deploys.has(Deploy.UPDATE_PURSE_PRICE.toString() + data.id)) {
            //Already processed
            continue;
          }
          */
          try {
            const termGenerator = deployTermGenerators.get(tag);
            console.info("currently deploying:");
            console.info(data);
            const term = termGenerator(data.payload);
            promises.push(rchainToolkit.http.easyDeploy(
              VALIDATOR_HOST,
              term,
              privateKey,
              1,
              10000000,
              10 * 60 * 1000
            ));
          }
          catch (err) {
            console.info(err);
            //TODO: retry?
            queue.push(data);
          }
        }

    }


  }
  console.info("deploying...");
  console.info(reverts);
  console.info(successMap);
  const ret = await Promise.all(promises);
  console.info("All done");
  console.info(ret);

  ret.forEach((deployRet, i) => {
    console.info(findTagById[i]);
    const data = rchainToolkit.utils.rhoValToJs(
      JSON.parse(deployRet).exprs[0].expr
    );
    console.info("data:");
    console.info(data);
    console.info("findTagById[i]", findTagById[i]);

    switch (findTagById[i]) {
      case Deploy.DEPLOY_BOX:
        console.info("deployed box");

        if (data.status === 'completed') {
          console.info("Box deployed!");
          deployQueue.delete(findTagById[i]);
        }
        else {
          reverts.get(findTagById[i]).at(i)();
        }

        return ret;
      case Deploy.CREATE_PURSES:
        console.info("created purses");
      
        if (data.status === 'completed') {
          console.info("Completed! :)");
          for (const [uniqueId, res] of Object.entries(data.results)) {
            if (res === false) {
              const deploy = deploys.get(findTagById[i].toString() + uniqueId);
              deploy.revert();
            } else {
              //successMap.get(findTagById[i]).at(i)();
            }
            //reverts.get(findTagById[i]).at(i)();
          }

          deployQueue.delete(findTagById[i]);
        }
        else {
          //reverts.get(findTagById[i]).at(i)();
        }

        return ret;
      case Deploy.DELETE_PURSE:
        console.info("deleted purse");

        if (data.status === 'completed') {
          //successMap.get(findTagById[i]).at(i)();
          deployQueue.delete(findTagById[i]);
        }
        else {
          reverts.get(findTagById[i]).at(i)();
        }

        return ret;
      case Deploy.CREDIT:
        console.info("credited");

        if (data.status === 'completed') {
          //successMap.get(findTagById[i]).at(i)();
          deployQueue.delete(findTagById[i]);
        }
        else {
          reverts.get(findTagById[i]).at(i)();
        }

        return ret;
      case Deploy.UPDATE_PURSE_DATA:
        console.info("updated purse data");

        if (data.status === 'completed') {
          //successMap.get(findTagById[i]).at(i)();
          deployQueue.delete(findTagById[i]);
        }
        else {
          reverts.get(findTagById[i]).at(i)();
        }

        return ret;
      case Deploy.UPDATE_PURSE_PRICE:
        console.info("updated purse price");

        if (data.status === 'completed') {
          //successMap.get(findTagById[i]).at(i)();
          deployQueue.delete(findTagById[i]);
        }
        else {
          reverts.get(findTagById[i]).at(i)();
        }

        return ret;
      case Deploy.WITHDRAW:
        console.info("withdrew");

        if (data.status === 'completed') {
          //successMap.get(findTagById[i]).at(i)();
          deployQueue.delete(findTagById[i]);
        }
        else {
          reverts.get(findTagById[i]).at(i)();
        }

        return ret;
      case Deploy.SWAP:
        console.info("swapped");

        if (data.status === 'completed') {
          //successMap.get(findTagById[i]).at(i)();
          deployQueue.delete(findTagById[i]);
        }
        else {
          reverts.get(findTagById[i]).at(i)();
        }

        return ret;
      default:
        console.info("unknown deploy tag");
    }
  })
  console.info(ret);
  console.info("deployed %d deploys", ret.length);
  return ret;
  
}

for (let i = 0; i < numberOfValidators; i++) {
  writeBundleQueue.registerHandler(async (data: QueueType) => {
    if (!pursesToRead.has(data)) {
      pursesToRead.add(data);
    }
    return data;
  });
}

const optionDefinitions = [
  { name: 'run', alias: 'r', type: Boolean },
  { name: 'clean', alias: 'c', type: Boolean },
  { name: 'masterRegUri', alias: 'u', type: String, multiple: false},
  { name: 'readOnlyHost', alias: 'o', type: String, multiple: true},
  { name: 'valodatorHost', alias: 'v', type: String, multiple: true},
  { name: 'command', type: String, multiple: false, defaultOption: true },
  { name: 'privKey', alias: 'p', type: String, multiple: false },
  { name: 'mnt', alias: 'm', type: String, multiple: false }
]


enum ExploreDeploy {
  READ_BOX = 1,
  READ_BOXES,
  READ_PURSES,
  READ_PURSES_DATA
}
enum Deploy {
  DEPLOY_BOX = 1,
  CREATE_PURSES,
  DELETE_PURSE,
  CREDIT,
  UPDATE_PURSE_DATA,
  UPDATE_PURSE_PRICE,
  WITHDRAW,
  SWAP
}


export type TermGeneratorFunction = (payload: any) => string;
const exploreDeployTermGenerators = new Map<ExploreDeploy, TermGeneratorFunction>([
  [ExploreDeploy.READ_BOX, readBoxTerm],
  [ExploreDeploy.READ_BOXES, readBoxesTerm],
  [ExploreDeploy.READ_PURSES, readPursesTerm],
  [ExploreDeploy.READ_PURSES_DATA, readPursesDataTerm]
]);
const deployTermGenerators = new Map<Deploy, TermGeneratorFunction>([
  [Deploy.DEPLOY_BOX, deployBoxTerm],
  [Deploy.CREATE_PURSES, createPursesTerm],
  [Deploy.DELETE_PURSE, deletePurseTerm],
  [Deploy.CREDIT, creditTerm],
  [Deploy.UPDATE_PURSE_DATA, updatePurseDataTerm],
  [Deploy.UPDATE_PURSE_PRICE, updatePursePriceTerm],
  [Deploy.WITHDRAW, withdrawTerm],
  [Deploy.SWAP, swapTerm]
]);
//const boxesBalancesMap = new Map<string, number>();

const bulkExploreDeploy = async function (tag: ExploreDeploy, payload: any, priority?: number) {
  const result = await readQueue.handle({
    tag: tag,
    payload: payload,
    priority: priority || 0
  });
  /*
  const termGenerator = exploreDeployTermGenerators.get(tag);
  const term = termGenerator(payload);
  const result = await rchainToolkit.http.exploreDeploy(READ_ONLY_HOST, {
    term: term
  });
  */
  return result;
}
const bulkDeploy = async function (tag: Deploy, payload: any, revert?: () => any, uniqueId?: string, success?: () => any, priority?: number) {
  const result = await writeQueue.handle({
    id: uniqueId,
    tag: tag,
    payload: payload,
    priority: priority || 0,
    revert: revert,
    success: success
  });
  return result;
  /*
  const termGenerator = deployTermGenerators.get(tag);
  const term = termGenerator(payload);
  rchainToolkit.http.easyDeploy(
    VALIDATOR_HOST,
    term,
    privateKey,
    1,
    10000000,
    10 * 60 * 1000
  );
  */
  //return result;
}


const options = commandLineArgs(optionDefinitions)
console.info(options);

const masterRegistryUri = options.masterRegUri;
const privateKey = options.privKey;
const mntPath = options.mnt;

const pubKey = rchainToolkit.utils.publicKeyFromPrivateKey(privateKey);


const ops = {
  init: async function(cb: FuseCallback) {
    console.info("INIT RDRIVE");

    const rootNode = mkDir("", "");
    setPathNode("", rootNode);
    setPathNode(pathSep, rootNode);

    //mkFile(pathSep, "token.conf", "{\n price: 0\n}\n");

    const result = await bulkExploreDeploy(ExploreDeploy.READ_BOXES, { masterRegistryUri: masterRegistryUri});
    const boxesResult = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);

    if (boxesResult) {
      console.info("boxes:");
      console.info(boxesResult);

      boxesResult.forEach((box: string) => {
        mkDir(pathSep, box);
      });
    }

    //TODO: check if master contract is deployed, if not deploy it
    return process.nextTick(cb, 0);
  },
  readdir: async function (inPath: string, cb: FuseCallback) {

    console.info("readdir");
    if (inPath !== '/') {
      const boxName = inPath.split("/")[1];
      const purseName = path.basename(inPath);

      const result = await bulkExploreDeploy(ExploreDeploy.READ_BOX, { masterRegistryUri: masterRegistryUri, boxId: boxName})

      const boxResult = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);

      if (boxResult) {
        const purses = boxResult.purses[contractName];
        const revPurses = boxResult.purses["rev"];
        console.info("Got purses: ");
        console.info(boxResult);
        if (revPurses && revPurses.length > 0) {
          console.info("has rev");

          const result = await bulkExploreDeploy(ExploreDeploy.READ_PURSES, {
            masterRegistryUri: masterRegistryUri,
            contractId: "rev",
            pursesIds: revPurses,
          });

          const purses = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);
          const balance = purses[revPurses[0]].quantity;

          const node = getPathNode(pathSep + boxName + pathSep + "revbalance.rev");

          if (!node) {
            console.info("making revbalance.rev");
            mkFile(pathSep + boxName, "revbalance.rev", balance.toString() + "\n");
          }
          else {
            //Update balance
            updateNodeContent(node, balance.toString() + "\n");
          }
        }
        if (purses) {
          purses.forEach((purse: string) => {
            const node = getPathNode(pathSep + boxName + pathSep + purse);

            if (!node) {
              mkFile(pathSep + boxName, purse, "");
            }
          });
        }
      }
    }

    const node = getPathNode(inPath);

    if (!node) {
      return process.nextTick(cb, Fuse.ENOENT);
    }
    if (!node.children) {
      return process.nextTick(cb, Fuse.ENOTDIR);
    }

    return process.nextTick(cb, 0, node.children.map((c: any) => c.name));
  },
  create: async function (inPath: string, mode: number, cb: FuseCallback) {
    console.info("create");

    let node: Node | null = null;
    const p = inPath.split(pathSep);
    const boxId = p[1];
    const name = p.pop();
    if (!name) {
      return process.nextTick(cb, Fuse.ENOENT);
    }

    if (name.length > getFsStats().max_name_length) {
      return process.nextTick(cb, Fuse.ENAMETOOLONG);
    }

    const result = await bulkExploreDeploy(ExploreDeploy.READ_BOX, { masterRegistryUri: masterRegistryUri, boxId: boxId});

    const boxResult = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);

    if (boxResult && boxResult.publicKey !== pubKey) {
      console.info("Can't create purse in a box that is not yours!");
      return process.nextTick(cb, Fuse.EACCES);
    }

    const extension = path.extname(name);
    if (extension !== ".mynft" && extension !== ".rev") {
      const payload = {
        masterRegistryUri,
        contractId: contractName,
        purses: {
          [name]: {
            id: name,
            price: null,
            boxId: boxId,
            quantity: 1
          },
        },
        data: {} 
      }

      bulkDeploy(Deploy.CREATE_PURSES, payload, () => {
        //Revert on failure
        console.info("Failed to create purse " + name);
        const node = getPathNode(inPath);
        if (node) {
          const parentPath = getPathFromName(inPath);
          const parentNode = getPathNode(parentPath);
          removeNodeFromParent(parentNode, node, inPath);
        }
      }, name);
    }


    if (isRegularFile(mode)) {
      node = mkFile(p.join(pathSep), name, "", mode);
    } else if (isDirectory(mode)) {
      node = mkDir(p.join(pathSep), name, mode);
    }
    if (!node) {
      return process.nextTick(cb, Fuse.ENOENT);
    }
    const f = mkFd(node);
    if (f < 0) {
      return process.nextTick(cb, Fuse.EMFILE);
    }
    return process.nextTick(cb, 0, f);
  },
  mknod: async function (path: string, mode: number, rdev: number, cb: FuseCallback) {
    console.info("mknod");
    return process.nextTick(cb, 0);
  },
  getattr: async function (inPath: string, cb: FuseCallback) {
    console.info("getattr " + inPath);

    const node = getPathNode(inPath);
    if (!node) {
      return process.nextTick(cb, Fuse.ENOENT);
    }
    return process.nextTick(cb, 0, node.stat);
  },
  open: function (inPath: string, flags: any, cb: FuseCallback) {
    return cb(0, 42)
  },
  release: function (inPath: string, fd: any, cb: FuseCallback) {
    return cb(0)
  },
  read: async function (inPath: string, fd: any, buf: any, len: number, pos: number, cb: FuseCallback) {
    const boxName = inPath.split("/")[1];
    const purseName = path.basename(inPath);

    const node = getPathNode(inPath);
    if (!node) {
      return process.nextTick(cb, Fuse.EBADF);
    }

    if (inPath.split("/").length > 2) {
      console.info("purseIds:");
      console.info([inPath.split("/")[2]]);

      const result = await bulkExploreDeploy(ExploreDeploy.READ_PURSES_DATA, {
        masterRegistryUri: masterRegistryUri,
        pursesIds: [inPath.split("/")[2]],
        contractId: contractName,
      });

      const retData = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);
      console.info(retData);

      if (retData[purseName]) {
        const content = Buffer.from(retData[purseName], 'base64').toString('utf-8')
        buf.write(content);

        const ret = updateNodeContent(node, content);
  
        return process.nextTick(cb, content.length);
        //return process.nextTick(cb, ret < 0 ? ret : len);
      }
      else {

        //return process.nextTick(cb, 0);
      }
      //return process.nextTick(cb, retData[purseName].length);
    }
    else {

    }
    //console.info("ret 0");
    //console.info(node.content);
    if (!node.content) return process.nextTick(cb, 0);
    const str = node.content.slice(pos, pos + len);
    if (!str) return process.nextTick(cb, 0);
    str.copy(buf);
    return process.nextTick(cb, str.length);

    //return cb(0);



    //const node = getFdNode(fd);

    //if (!node.content) return process.nextTick(cb, 0);
    //const str = node.content.slice(pos, pos + len);
    //if (!str) return process.nextTick(cb, 0);
    //str.copy(buf);
    //return process.nextTick(cb, str.length);

    /*

    if (inPath.split("/").length > 2) {

      const term2 = readPursesDataTerm({
        masterRegistryUri: masterRegistryUri,
        pursesIds: [inPath.split("/")[2]],
        contractId: contractName,
      });

      const result = await rchainToolkit.http.exploreDeploy(READ_ONLY_HOST, {
        term: term2,
      });

      const retData = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);
      console.info(retData);


      buf.write(retData[purseName]);
      return cb(retData[purseName].length);
    }

    return cb(0);
    */
  },
  truncate: async function (inPath: string, size: any, cb: FuseCallback) {
    const node = getPathNode(inPath);
    if (!node) {
      return process.nextTick(cb, Fuse.ENOENT);
    }
    truncateFile(node, size);
    return process.nextTick(cb, 0);
  },
  ftruncate: async function (inPath: string | Buffer, fd: number, size: any, cb: FuseCallback) {
    const node = getPathNode(inPath.toString());
    if (!node) {
      //if (debugLevel === "trace")
        console.log("ftruncate fd not found!");
      return process.nextTick(cb, Fuse.EBADF);
    }
    truncateFile(node, size);
    return process.nextTick(cb, 0);
  },
  utimens: function (
    inPath: string,
    atime: number,
    mtime: number,
    cb: FuseCallback
  ) {
    const node = getPathNode(inPath);
    if (!node) {
      return process.nextTick(cb, Fuse.ENOENT);
    }
    node.stat.atime = atime;
    node.stat.mtime = mtime;
  
    return process.nextTick(cb, 0);
  },
  unlink: async function (inPath: string, cb: FuseCallback) {
    if (inPath === "." || inPath === "..") {
      return process.nextTick(cb, Fuse.EINVAL);
    }
    const node = getPathNode(inPath);
    if (!node) {
      return process.nextTick(cb, Fuse.ENOENT);
    }

    const boxName = inPath.split("/")[1];
    const purseName = path.basename(inPath);
    let payload = {
      masterRegistryUri: masterRegistryUri,
      contractId: contractName,
      purseId: purseName,
    };
    bulkDeploy(Deploy.DELETE_PURSE, payload);

    const parentPath = getPathFromName(inPath);
    const parentNode = getPathNode(parentPath);
    if (!parentNode) {
      //if (debugLevel === "trace")
        console.error(
          "unlink unable to find parent node (%s, %s)",
          path,
          parentPath
        );
      return process.nextTick(cb, Fuse.ENOENT);
    }
    removeNodeFromParent(parentNode, node, inPath);
    return process.nextTick(cb, 0);
  },
  write: async function (inPath: string, fd: any, buf: any, length: number, pos: number, cb: FuseCallback) {
    const boxName = inPath.split("/")[1];
    const purseName = path.basename(inPath);
    const extension = path.extname(purseName);

    const node = getPathNode(inPath);
    if (!node) {
      return process.nextTick(cb, Fuse.EBADF);
    }
  
    let content: Buffer;
    if (!node.content) {
      node.content = Buffer.alloc(length + pos);
    }
    if (pos) {
      content = Buffer.concat([
        node.content.slice(0, pos),
        buf.slice(0, length)
      ]);
    } else {
      content = Buffer.from(buf.slice(0, length));
    }

    const boxNode = getPathNode(pathSep + boxName);
    if (boxNode && !boxNode.owner) {
      const result = await bulkExploreDeploy(ExploreDeploy.READ_BOX, { masterRegistryUri: masterRegistryUri, boxId: boxName});
      const boxResult = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);

      if (boxResult && boxResult.publicKey !== pubKey) {
        console.info("Can't update data in a box that is not yours!");
        return process.nextTick(cb, Fuse.EACCES);
      }

      if (boxResult) {
        changeNodeOwner(boxNode, boxName)
      }
    }


    if (extension !== ".mynft") {
      if (extension === ".rev") {
        console.info("updating rev balance...");
        const result = await bulkExploreDeploy(ExploreDeploy.READ_BOX, { masterRegistryUri: masterRegistryUri, boxId: boxName});
        const boxResult = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);
  
        if (boxResult && boxResult.publicKey !== pubKey) {
          console.info("Can't update data in a box that is not yours!");
          return process.nextTick(cb, Fuse.EACCES);
        }
        
        if (boxResult && boxResult.purses && boxResult.purses.hasOwnProperty(contractName)) {
          const purses = boxResult.purses[contractName];
          const revPurses = boxResult.purses["rev"];

          console.info("Got purses: ");
          console.info(boxResult);

          let currentBalance = 0;

          if (revPurses && revPurses.length > 0) {
            console.info("has rev");

            const result = await bulkExploreDeploy(ExploreDeploy.READ_PURSES, {
              masterRegistryUri: masterRegistryUri,
              contractId: "rev",
              pursesIds: revPurses,
            });

            const purses = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);
            currentBalance = purses[revPurses[0]].quantity.toString();
          }

          const desiredBalance = parseInt(content.toString());
          const topUp = desiredBalance - currentBalance;
          
          if (topUp > 0) {
            console.info("top up...");
            const payload = {
              revAddress: rchainToolkit.utils.revAddressFromPublicKey(pubKey),
              quantity: topUp,
              masterRegistryUri: masterRegistryUri,
              boxId: boxName
            }
            bulkDeploy(Deploy.CREDIT, payload);
          }
          if (topUp < 0) {
            //Do withdraw
            console.info("Withdrawing... ");
            const payload = {
              masterRegistryUri: masterRegistryUri,
              withdrawQuantity: Math.abs(topUp),
              purseId: revPurses[0],
              toBoxId: "_rev",
              boxId: boxName,
              contractId: "rev",
              merge: false,
            };
            bulkDeploy(Deploy.WITHDRAW, payload);
          }
        }

      } else {
        const payload = {
          masterRegistryUri: masterRegistryUri,
          purseId: purseName,
          boxId: boxName,
          contractId: contractName,
          data: Buffer.from(content).toString("base64"),
        };

        try {
          bulkDeploy(Deploy.UPDATE_PURSE_DATA, payload, () => {}, purseName);

          console.info("File saved!");
          cb(length)
        } catch (err) {
          console.log(err);
          cb(0);
        }
      }
    }
    else {
        //Update purse config
        const conf = JSON.parse(content.toString());
        const purseId = purseName.slice(0, -4);
        let price = null;

        if (conf.price) {
          price = conf.price;
        }
        const payload = {
          masterRegistryUri: masterRegistryUri,
          purseId: purseId,
          boxId: boxName,
          contractId: contractName,
          price: `"rev", ${price}`,
        };
        bulkDeploy(Deploy.UPDATE_PURSE_PRICE, payload, () => {}, purseId);
    }

    const ret = updateNodeContent(node, content);
  
    return process.nextTick(cb, ret < 0 ? ret : length);
  },
  rename: async function(src: string, dst: string, cb: FuseCallback) {
    console.info("rename");

    const srcBoxName = src.split("/")[1];
    const srcPurseName = path.basename(src);

    const dstBoxName = dst.split("/")[1];
    const dstPurseName = path.basename(dst);

    if(srcBoxName === dstBoxName || srcPurseName !== dstPurseName) {
      return process.nextTick(cb, Fuse.ENOENT);
    }

    const purseName = srcPurseName;
    const extension = path.extname(srcPurseName);

    const srcNode = getPathNode(src);
    if (!srcNode) {
      return process.nextTick(cb, Fuse.ENOENT);
    }

    if (extension !== ".mynft") {
      //Check if we own the src box
      const result = await bulkExploreDeploy(ExploreDeploy.READ_BOX, { masterRegistryUri: masterRegistryUri, boxId: srcBoxName});

      const boxResult = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);

      if (boxResult) {
        const boxPublicKey = boxResult.publicKey;
        if (boxPublicKey !== pubKey) {
            //Do purchase
            const payload = {
              masterRegistryUri: masterRegistryUri,
              purseId: purseName,
              contractId: contractName,
              boxId: dstBoxName,
              quantity: 1,
              merge: true,
              data: '',
              newId: dstPurseName,
            }
            bulkDeploy(Deploy.SWAP, payload);

        } else {
            let quantity = 1;
            let purseId = purseName;
            let currentBalance = 0;
            if (extension === ".rev" && srcNode.content) {
              quantity = parseInt(srcNode.content.toString());

              const result = await bulkExploreDeploy(ExploreDeploy.READ_BOX, { masterRegistryUri: masterRegistryUri, boxId: srcBoxName});
              const boxResult = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);
        
              if (boxResult && boxResult.publicKey !== pubKey) {
                console.info("Can't move rev from a purse that isn't yours!");
                return process.nextTick(cb, Fuse.EACCES);
              }
              
              if (boxResult && boxResult.purses && boxResult.purses.hasOwnProperty("rev")) {
                //const purses = boxResult.purses[contractName];
                const revPurses = boxResult.purses["rev"];
                purseId = revPurses[0];
                console.info("boxResult:");
                console.info(boxResult);

                if (revPurses && revPurses.length > 0) {
                  console.info("has rev");
      
                  const result = await bulkExploreDeploy(ExploreDeploy.READ_PURSES, {
                    masterRegistryUri: masterRegistryUri,
                    contractId: "rev",
                    pursesIds: revPurses,
                  });
      
                  const purses = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);
                  currentBalance = purses[revPurses[0]].quantity;
                }
              }
            }
            if (quantity > 0) {
              //Do withdraw
              const payload = {
                masterRegistryUri: masterRegistryUri,
                withdrawQuantity: quantity,
                purseId: purseId,
                toBoxId: dstBoxName,
                boxId: srcBoxName,
                contractId: extension === ".rev" ? "rev" : contractName,
                merge: false,
              };
              bulkDeploy(Deploy.WITHDRAW, payload, () => {
                console.info("transaction unsuccessful! :(");
                //Revert to previous balance
                mkFile(src, purseName, currentBalance.toString());
              }, purseName, () => {
                console.info("transaction successful! :)");
              });
            }
        }
      }

    }
    if (extension !== ".rev") {
      const srcParentPath = getPathFromName(src);
      const srcParentNode: Node | null = getPathNode(srcParentPath);
      if (!srcParentNode) {
        return process.nextTick(cb, Fuse.ENOENT);
      }
    
      const dstName = getNameFromPath(dst);
      if (dstName.length > getFsStats().max_name_length) {
        return process.nextTick(cb, Fuse.ENAMETOOLONG);
      }
      const dstNode = getPathNode(dst);
      const dstParentPath = getPathFromName(dst);
      const dstParentNode: Node | null = getPathNode(dstParentPath);
      if (!dstParentNode) {
        return process.nextTick(cb, Fuse.ENOENT);
      }
    
      if (dstNode && dstParentNode) {
        removeNodeFromParent(dstParentNode, dstNode, dst);
      }
      removeNodeFromParent(srcParentNode, srcNode, src);
      addNodeToParent(dstParentNode, srcNode, dstName, dst);
    } else {
      const parentPath = getPathFromName(src);
      const parentNode = getPathNode(parentPath);
      if (parentNode) {
        removeNodeFromParent(parentNode, srcNode, src);
      }
    }
    return process.nextTick(cb, 0);
  },
  mkdir: async function (inPath: string, mode: number, cb: FuseCallback) {
    console.info("mkdir");
    console.info(inPath);
    console.info(mode);

    let node: Node | null = null;
    const p = inPath.split(pathSep);
    const name = p.pop();
    
    if (p.length > 1) {
      return process.nextTick(cb, Fuse.ENOENT);
    }

    if (!name) {
      return process.nextTick(cb, Fuse.ENOENT);
    }
    if (name.length > getFsStats().max_name_length) {
      return process.nextTick(cb, Fuse.ENAMETOOLONG);
    }

    const payload = {
      publicKey: pubKey,
      revAddress: rchainToolkit.utils.revAddressFromPublicKey(pubKey),
      boxId: name,
      masterRegistryUri: masterRegistryUri,
    }

    try {
      bulkDeploy(Deploy.DEPLOY_BOX, payload);
    } catch (err) {
      console.log(err);
      return process.nextTick(cb, Fuse.ENOENT);
    }

    node = mkDir(p.join(pathSep), name, mode | constants.S_IFDIR);
    if (!node) {
      return process.nextTick(cb, Fuse.ENOENT);
    }
    const f = mkFd(node);
    if (f < 0) {
      return process.nextTick(cb, Fuse.EMFILE);
    }
    return process.nextTick(cb, 0, f);
  }
}


let fuse2: Fuse | null = null;

//TODO: fix os-service linux script
/*
if (options.up) {
  service.add ("rdrive-service", {nodePath: process.argv[0], programPath: "", programArgs: ["--run"]}, function(error: any){ 
      if (error)
        console.trace(error);
  });
}

else if (options.down) {
  service.remove("rdrive-service", function(error: any){ 
    if (error)
        console.trace(error);
  });
}
*/

if (options.run) {
  console.info("With --run");
  fuse2 = new Fuse(mntPath, ops, { displayFolder: "RDrive", mkdir: true, debug: false });
  let deployBundlerInterval = setInterval(deployBundler, 20000);

  service.run (function () {
    fuse2?.unmount( () => {
      logStream.write("Unmounted" + "\n");
      //TODO: Wait until all deploys are sent
      setTimeout(() => {
        clearInterval(deployBundlerInterval);
        service.stop(0);
      }, 2000);
    });
  });


  
  logStream.write("Running service" + "\n");
  fuse2.mount(function (err: any) {
    logStream.write("Mounted" + "\n");
  })
  

  process.on('SIGINT', () => {
      console.info("Unmounting...");
      fuse2?.unmount( () => {
        logStream.write("Unmounted" + "\n");
        //TODO: Wait until all deploys are sent
        setTimeout(() => {
          clearInterval(deployBundlerInterval);
          service.stop(0);
        }, 2000);
        process.exit(0);
      });
  });
}
else if (options.clean) {
  if (fuse2) {
    console.info("Unmounting2...")
    fuse2.unmount( () => {
      logStream.write("Unmounted" + "\n");
    });
  }
}
else {
  console.info("Usage: rdrive-service up|down");
}
