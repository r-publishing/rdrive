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
  getFdNode,
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
  removeNodeFromParent,
  adjustBlocksUsed
} from "./cache";

import { sep as pathSep } from 'path';
import { constants } from 'fs';
import workerpool from 'workerpool';
import { result } from "lodash";
import { dataAtName } from "rchain-toolkit/dist/http";

const require = createRequire(import.meta.url);

const {
  masterTerm,
  deployTerm,
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

//process.setMaxListeners(0);
require('events').EventEmitter.defaultMaxListeners = 1000;
 
 
const message_file_path = path.join(import.meta.url, '../assets/message.json');
const fuse_node_path = path.join(import.meta.url, '../assets/fuse.node');
const libfuse_path = path.join(import.meta.url, '../assets/libfuse.so');


//const messageFileURL = new URL("../assets/message.json", import.meta.url);
//const fileBuff = fs.readFileSync(messageFileURL.pathname);
//const { value } = JSON.parse(fileBuff.toString());

//const cloneDeep = require('lodash.cloneDeep');
const QueuedJobs = require("queued-jobs").default;
const readQueue = new QueuedJobs(40960, 2000000);
const writeQueue = new QueuedJobs(40960, 2000000);

const readBundleQueue = new QueuedJobs(40960, 2000000);
const writeBundleQueue = new QueuedJobs(40960, 2000000);
const maxPhloLimit = 1000000000;

const pursesToRead = new Set();

type DeployType = {
  id: string;
  deployType: number;
  payload: any;
  priority: number;
  revert: () => void;
  success: () => void;
};

type BundledDeployType = {
  payload: any;
  termGenerator: (any) => string;
};

const deployQueue = new Map<number, Array<DeployType>>();
const exploreDeployQueue = new Map<number, Array<DeployType>>();

//const deployPool = workerpool.pool();


readQueue.registerHandler(async (data: DeployType) => {
  const termGenerator = exploreDeployTermGenerators.get(data.deployType);
  const term = termGenerator(data.payload);
  const result = await rchainToolkit.http.exploreDeploy(READ_ONLY_HOST, {
    term: term
  });
  
  return result;
});

readBundleQueue.registerHandler(async (data: DeployType) => {
  if (!pursesToRead.has(data)) {
    pursesToRead.add(data);
  }
  return data;
});

var logStream = fs.createWriteStream(process.argv[0] + ".log");

const VALIDATOR_HOST = "http://127.0.0.1:40403";
const validatorArray = [VALIDATOR_HOST];
const READ_ONLY_HOST = "http://127.0.0.1:40403";
const numberOfValidators = 1;
const contractName = "mynft";

writeQueue.registerHandler(async (data: DeployType) => {
  if (!deployQueue.has(data.deployType)) {
    deployQueue.set(data.deployType, [data]);
  } else {
    deployQueue.get(data.deployType).push(data);
  }
})


const deployBundler = async function(deployBundle: Map<number, Array<DeployType>>) {
  const promises = [];
  const reverts = new Map<number, Array<() => any>>();
  const successMap = new Map<number, Array<() => any>>();
  const deploys = new Map<string, DeployType>();
  let findDeployTypeById = Array<number>();

  for (const [deployType, queue] of deployBundle) {
    findDeployTypeById.push(deployType);
    if (!reverts.has(deployType)) {
      reverts.set(deployType, []);
    }
    if (!successMap.has(deployType)) {
      successMap.set(deployType, []);
    }
    const queueLength = queue.length;

    switch (deployType) {
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
          reverts.get(deployType).push(data.revert);
          successMap.get(deployType).push(data.success);
          deploys.set(deployType.toString() + data.id, data);
        }

        if (deployBundle.has(Deploy.UPDATE_PURSE_DATA)) {
          console.info("there is data");
          const dataQueue = deployBundle.get(Deploy.UPDATE_PURSE_DATA);
          for (var i = dataQueue.length - 1; i >= 0; i--) {
            
            const dataDeploy = dataQueue.at(i);
            //console.info(dataDeploy);


            //console.info("data.payload.length: ", dataDeploy.payload.length);
            //console.info("data.payload.pos: ", dataDeploy.payload.pos);

            const chunkId = dataDeploy.payload.pos / 4096;

            //payload3.data = {...payload3.data, [chunkId]: data.payload.data};

            if (!Object.keys(payload.purses).includes(dataDeploy.payload.purseId)) {
              continue;
            }

            //deploy would be too large
            if (payload.data[dataDeploy.payload.purseId] && Object.keys(payload.data[dataDeploy.payload.purseId]).length > 4096 ) {
              break
            }

            //console.info("dataDeploy.id in payload.purses ");
            //console.info(dataDeploy.payload);

            payload.data[dataDeploy.payload.purseId] = {...payload.data[dataDeploy.payload.purseId], [chunkId]: dataDeploy.payload.data};
            
            if (!reverts.has(Deploy.UPDATE_PURSE_DATA)) {
              reverts.set(Deploy.UPDATE_PURSE_DATA, [dataDeploy.revert]);
            } else {
              reverts.get(Deploy.UPDATE_PURSE_DATA).push(dataDeploy.revert);
            }
            deploys.set(Deploy.UPDATE_PURSE_DATA.toString() + dataDeploy.id, dataDeploy);
            //deployBundle.set(Deploy.UPDATE_PURSE_DATA, dataQueue.filter( el => el.id !== dataDeploy.id));
          }
        }
        /*
        if (deployBundle.has(Deploy.UPDATE_PURSE_PRICE)) {
          const priceQueue = deployBundle.get(Deploy.UPDATE_PURSE_PRICE);

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
            deployBundle.set(Deploy.UPDATE_PURSE_PRICE, priceQueue.filter( el => el.id !== priceDeploy.id));
          }
        }
        */

        try {
          const termGenerator = deployTermGenerators.get(deployType);
          promises.push(writeBundleQueue.handle({
            termGenerator: termGenerator,
            payload: payload
          }));
        }
        catch (err) {
          console.info(err);
          //TODO: retry?
        }

        deployBundle.delete(deployType);
        continue;

      case Deploy.UPDATE_PURSE_DATA:
          //console.info("UPDATE_PURSE_DATA");
          let processedChunks = [];
          //Group deploys by name
          /*
          const groupedDeploys = queue.reduce((groups: Map<string, DeployType[]>, item: DeployType) => ({
            ...groups,
            [item.id]: [...(groups[item.id] || []), item]
          }), new Map<string, DeployType[]>());
          */

          var groupedByName = queue.reduce(function(groups: Map<string, DeployType[]>, item: DeployType) {
            if (!groups.has(item.payload.purseId)) {
              groups.set(item.payload.purseId, []);
            }
            groups.get(item.payload.purseId).push(item);
            return groups;
          }, new Map<string, DeployType[]>());



          for (const [purseId, queue2] of groupedByName) {
            var groupedByBoxId = queue2.reduce(function(groups: Map<string, DeployType[]>, item: DeployType) {
              if (!groups.has(item.payload.boxId)) {
                groups.set(item.payload.boxId, []);
              }
              groups.get(item.payload.boxId).push(item);
              return groups;
            }, new Map<string, DeployType[]>());

            for (const [boxId, queue3] of groupedByBoxId) {
              const node = getPathNode(pathSep + boxId + pathSep + purseId);
              if (!node) {
                continue;
              }

              if (!node.isFinalized) {
                continue;
              }

              const queueLength3 = queue3.length;

              let payload3 = {
                masterRegistryUri: masterRegistryUri,
                purseId: purseId,
                boxId: boxId,
                contractId: contractName,
                data: {}
              };

              for (var i = 0; i < queueLength3; i++) {
                const data = queue3.at(i);

                const chunkId = data.payload.pos / 4096;

                //deploy would be too large
                if (Object.keys(payload3.data).length > 4096 ) {
                  break
                }

                payload3.data = {...payload3.data, [chunkId]: data.payload.data};
                
                reverts.get(data.deployType).push(data.revert);
                successMap.get(data.deployType).push(data.success);
                deploys.set(data.deployType.toString() + data.id, data);
              
                deployBundle.set(Deploy.UPDATE_PURSE_DATA, queue.filter( el => el.id !== data.id));
                processedChunks.push(chunkId);
              }


              payload3.data = JSON.stringify(payload3.data);
              const termGenerator = deployTermGenerators.get(deployType);

              console.info("Deploying Chunks: ");
              console.info(processedChunks);
              
              promises.push(writeBundleQueue.handle({
                termGenerator: termGenerator,
                payload: payload3
              }));

            }
          }

          continue;
      // TODO:
      //case Deploy.UPDATE_PURSE_PRICE:
      //    continue
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
            const termGenerator = deployTermGenerators.get(deployType);
            promises.push(writeBundleQueue.handle({
              termGenerator: termGenerator,
              payload: data.payload
            }));
          }
          catch (err) {
            console.info(err);
            //TODO: retry?
            queue.push(data);
          }
        }

        deployBundle.delete(deployType);

    }


  }

  let ret = [];
  if (promises.length > 0) {
    ret = await Promise.all(promises);
    console.info("All done");
    console.info(ret);
  }
  else {
    //Wait a minimum of 1 second.
    //await new Promise(resolve => setTimeout(resolve, 1000));
    return;
  }

  ret.forEach((deployRet, i) => {
    const deployType = findDeployTypeById[i];
    const data = rchainToolkit.utils.rhoValToJs(
      JSON.parse(deployRet).exprs[0].expr
    );

    switch (deployType) {
      case Deploy.DEPLOY_BOX:
        if (data.status === 'completed') {
          console.info("Box deployed!");
          deployBundle.delete(deployType);
        }
        else {
          reverts.get(deployType).at(i)();
        }

        return ret;
      case Deploy.CREATE_PURSES:
        if (data.status === 'completed') {
          console.info("created purses");
          for (const [uniqueId, res] of Object.entries(data.results)) {
            if (res === false) {
              const deploy = deploys.get(deployType.toString() + uniqueId);
              deploy.revert();
            } else {
              successMap.get(deployType).at(i)();
            }
          }

          deployBundle.delete(deployType);
        }
        else {
          //reverts.get(deployType).at(i)();
        }

        return ret;
      case Deploy.DELETE_PURSE:

        if (data.status === 'completed') {
          console.info("deleted purse");
          //successMap.get(deployType).at(i)();
          deployBundle.delete(deployType);
        }
        else {
          reverts.get(deployType).at(i)();
        }

        return ret;
      case Deploy.CREDIT:

        if (data.status === 'completed') {
          console.info("credited");
          //successMap.get(deployType).at(i)();
          deployBundle.delete(deployType);
        }
        else {
          reverts.get(deployType).at(i)();
        }

        return ret;
      case Deploy.UPDATE_PURSE_DATA:

        if (data.status === 'completed') {
          console.info("updated purse data");
          //successMap.get(deployType).at(i)();
          //deployBundle.delete(deployType);
        }
        else {
          reverts.get(deployType).at(i)();
        }

        return ret;
      case Deploy.UPDATE_PURSE_PRICE:

        if (data.status === 'completed') {
          console.info("updated purse price");
          //successMap.get(deployType).at(i)();
          deployBundle.delete(deployType);
        }
        else {
          reverts.get(deployType).at(i)();
        }

        return ret;
      case Deploy.WITHDRAW:

        if (data.status === 'completed') {
          console.info("withdrew");
          //successMap.get(deployType).at(i)();
          deployBundle.delete(deployType);
        }
        else {
          reverts.get(deployType).at(i)();
        }

        return ret;
      case Deploy.SWAP:

        if (data.status === 'completed') {
          console.info("swapped");
          //successMap.get(deployType).at(i)();
          deployBundle.delete(deployType);
        }
        else {
          reverts.get(deployType).at(i)();
        }

        return ret;
      default:
        console.info("unknown deploy deployType");
    }
  })

  if (ret.length > 0) {
    console.info(ret);
    console.info("deployed %d deploys", ret.length);
  }
  return ret;
  
}

for (let i = 0; i < 1000; i++) { //
  writeBundleQueue.registerHandler(async (data: BundledDeployType) => {
    console.info("writeBundleDeployHandler................................");
    const term = data.termGenerator(data.payload);
    //console.info(term);
    const ret = await rchainToolkit.http.easyDeploy(
      validatorArray[i % numberOfValidators],
      term,
      privateKey,
      1,
      maxPhloLimit,
      10 * 60 * 1000
    ).catch(err => {
      console.info("Cought error");
      console.info(err);
    });

    return ret;
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
  { name: 'boxName', alias: 'b', type: String, multiple: false },
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

const bulkExploreDeploy = async function (deployType: ExploreDeploy, payload: any, priority?: number) {
  const result = await readQueue.handle({
    deployType: deployType,
    payload: payload,
    priority: priority || 0
  });
  /*
  const termGenerator = exploreDeployTermGenerators.get(deployType);
  const term = termGenerator(payload);
  const result = await rchainToolkit.http.exploreDeploy(READ_ONLY_HOST, {
    term: term
  });
  */
  return result;
}
const bulkDeploy = async function (deployType: Deploy, payload: any, revert?: () => any, uniqueId?: string, success?: () => any, priority?: number) {
  const result = await writeQueue.handle({
    id: uniqueId,
    deployType: deployType,
    payload: payload,
    priority: priority || 0,
    revert: revert,
    success: success
  });
  return result;
  /*
  const termGenerator = deployTermGenerators.get(deployType);
  const term = termGenerator(payload);
  rchainToolkit.http.easyDeploy(
    VALIDATOR_HOST,
    term,
    privateKey,
    1,
    maxPhloLimit,
    10 * 60 * 1000
  );
  */
  //return result;
}


const options = commandLineArgs(optionDefinitions)
console.info(options);

let masterRegistryUri = options.masterRegUri;
const privateKey = options.privKey;
const mntPath = options.mnt;
const defaultBoxName = options.boxName;

let pubKey = rchainToolkit.utils.publicKeyFromPrivateKey(privateKey);


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
      boxesResult.forEach((box: string) => {
        const folderNode = mkDir(pathSep, box);
        const f = mkFd(folderNode);
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

      if (boxResult && boxResult.purses) {
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
        if (purses && purses.length > 0) {
          console.info("purses:");
          console.info(purses);
          const pursesResult = await bulkExploreDeploy(ExploreDeploy.READ_PURSES, {
            masterRegistryUri: masterRegistryUri,
            contractId: contractName,
            pursesIds: purses,
          });
          const pursesInfo = rchainToolkit.utils.rhoValToJs(JSON.parse(pursesResult).expr[0]);
          console.info(pursesInfo);

          purses.forEach((purse: string) => {
            const node = getPathNode(pathSep + boxName + pathSep + purse);

            if (!node) {
              const fileNode = mkFile(pathSep + boxName, purse, "");
              fileNode.isFinalized = true;

              const purseInfo = pursesInfo[purse];
              const nOfChunks = purseInfo.chunks || 1;
              const prevBlocksSize = fileNode.stat.blocks;
              fileNode.stat.blocks = nOfChunks;
              fileNode.stat.size = nOfChunks * 4096;
              fileNode.content = Buffer.alloc(fileNode.stat.size);
              if (prevBlocksSize !== fileNode.stat.blocks) {
                adjustBlocksUsed(fileNode.stat.blocks - prevBlocksSize);
              }

              const f = mkFd(fileNode);
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
    const isHiddenFile = name.startsWith(".");
    
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
    if (!isHiddenFile && extension !== ".rev") {
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
        console.info("FAILED to create purse " + name);
        const node2 = getPathNode(inPath);
        if (node2) {
          const parentPath = getPathFromName(inPath);
          const parentNode = getPathNode(parentPath);
          removeNodeFromParent(parentNode, node2, inPath);
        }
      }, name, () => {
        const node2 = getPathNode(inPath);
        if (node2) {
          node2.isFinalized = true;
        }
        console.info("PURSE CREATED");
        console.info(inPath);
      });
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
  fgetattr: function (fd: number, cb: FuseCallback) {
    console.log("fgetattr(%d)", fd);
    const node = getFdNode(fd);
    if (!node) {
      return process.nextTick(cb, Fuse.EBADF);
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
    const chunkStart = pos / 4096;
    const chunksLength = len / 4096;
    const boxName = inPath.split("/")[1];

    const purseName = path.basename(inPath);
    const node = getPathNode(inPath);
    //const node = getFdNode(fd);
    if (!node) {
      return process.nextTick(cb, Fuse.EBADF);
      /*
      console.info("Not found in cache");
      //See if we can find it
      const result = await bulkExploreDeploy(ExploreDeploy.READ_BOX, { masterRegistryUri: masterRegistryUri, boxId: boxName});

      const boxResult = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);
  
      if (boxResult && boxResult.purses && boxResult.purses.hasOwnProperty(contractName)) {
        const purses = boxResult.purses[contractName];
        if (!purses.includes(purseName)) {
          return process.nextTick(cb, Fuse.EBADF);
        }
      }
      */
    }

    const chunksToRead = [...Array(chunksLength).keys()].map(i => i + chunkStart).filter(i => i < node.stat.blocks);

    if (inPath.split("/").length > 2 && chunksToRead.length > 0) {
      const result = await bulkExploreDeploy(ExploreDeploy.READ_PURSES_DATA, {
        masterRegistryUri: masterRegistryUri,
        pursesIds: [purseName],
        contractId: contractName,
        chunks: {
          [purseName] : chunksToRead
        }
      });

      console.info("reading chunks: ");
      console.info(chunksToRead );
      
      const retData = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);

      if (retData[purseName]) {
        /*
        let totalLength = 0;

        Object.keys(retData[purseName]).forEach((chunkId: string) => {
          const data = retData[purseName][chunkId];
          const chunkContent = Buffer.from(data, 'base64url');
          totalLength += chunkContent.length;
        });
        */

        var b = Buffer.alloc(len);
        Object.keys(retData[purseName]).forEach((chunkId: string) => {
          const data = retData[purseName][chunkId];
          const dataAsStr = Buffer.from(data, "base64url");
          const chunkb = Buffer.from(data.toString("utf8"), 'base64url');
          chunkb.copy(b, (parseInt(chunkId) - chunkStart) * 4096, 0, dataAsStr.length);
        });


        //const str = b.slice(pos, pos + len);
        const chunkData = b.slice(0, len);
        const str = Buffer.concat([
          node.content.slice(0, pos),
          chunkData
        ]);
        chunkData.copy(buf);

        const ret = updateNodeContent(node, str);

        return process.nextTick(cb, ret < 0 ? ret : len);

      }
      else {
        //return process.nextTick(cb, 0);
      }
      //return process.nextTick(cb, retData[purseName].length);
    }

    if (!node.content) {
      return process.nextTick(cb, 0);
    }
    const str = node.content.slice(pos, pos + len);
    if (!str) {
      return process.nextTick(cb, 0);
    }

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
    console.info("ftruncate", size);
    const node = getPathNode(inPath.toString());
    //const node = getFdNode(fd);
    if (!node) {
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
      return process.nextTick(cb, Fuse.ENOENT);
    }
    removeNodeFromParent(parentNode, node, inPath);
    return process.nextTick(cb, 0);
  },
  write: async function (inPath: string, fd: any, buf: any, length: number, pos: number, cb: FuseCallback) {
    const boxName = inPath.split("/")[1];
    const purseName = path.basename(inPath);
    const extension = path.extname(purseName);
    const isHiddenFile = purseName.startsWith(".");

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


    if (!isHiddenFile) {
      if (extension === ".rev") {
        console.info("updating rev balance...");
        const result = await bulkExploreDeploy(ExploreDeploy.READ_BOX, { masterRegistryUri: masterRegistryUri, boxId: boxName});
        const boxResult = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);
  
        if (boxResult && boxResult.publicKey !== pubKey) {
          console.info("Can't update data in a box that is not yours!");
          return process.nextTick(cb, Fuse.EACCES);
        }
        
        if (boxResult && boxResult.purses) {
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
          data: Buffer.from(buf.slice(0, length), "utf8").toString("base64url"), //Buffer.from(content),
          //Additional chunk data
          length: length,
          pos: pos,
        };

        try {
          bulkDeploy(Deploy.UPDATE_PURSE_DATA, payload, () => {}, purseName+"@"+boxName, () => {
            console.info("DATA UPDATED");
          });

          //console.info("File saved!");
          cb(length)
        } catch (err) {
          console.log(err);
          cb(0);
        }
      }
    }
    else {
        //Update purse config
        console.info("Update purse config");
        const conf = JSON.parse(content.toString());
        const purseId = purseName.slice(0, -(contractName.length+1)).substring(1);
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
    const isHiddenFile = purseName.startsWith(".");

    const srcNode = getPathNode(src);
    if (!srcNode) {
      return process.nextTick(cb, Fuse.ENOENT);
    }

    if (!isHiddenFile) {
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

function flush() {
  process.stdout.clearLine(-1);
  process.stdout.cursorTo(0);
}

const runFunction = async function () {
  if (!masterRegistryUri) {
    console.info("No master registry uri, creating a new one");
    const term = masterTerm({
      depth: 3,
      contractDepth: 2,
    });

    const dataAtNameResponse = await rchainToolkit.http.easyDeploy(
      VALIDATOR_HOST,
      term,
      privateKey,
      1,
      maxPhloLimit,
      10 * 60 * 1000
    );
  
    const data = rchainToolkit.utils.rhoValToJs(
      JSON.parse(dataAtNameResponse).exprs[0].expr
    );
  
    masterRegistryUri = data.registryUri.replace('rho:id:', '');
    console.info("Master Registry URI: %s", masterRegistryUri);

    //Deploy box
    const term2 = deployBoxTerm({
      masterRegistryUri: masterRegistryUri,
      boxId: defaultBoxName,
      publicKey: pubKey,
      revAddress: rchainToolkit.utils.revAddressFromPublicKey(pubKey),
    });

    const dataAtNameResponse2 = await rchainToolkit.http.easyDeploy(
      VALIDATOR_HOST,
      term2,
      privateKey,
      1,
      maxPhloLimit,
      10 * 60 * 1000
    );
  
    const data2 = rchainToolkit.utils.rhoValToJs(
      JSON.parse(dataAtNameResponse2).exprs[0].expr
    );
  
    if (data2.status !== 'completed') {
      console.info("Unable to create box.");
      return;
    }

    //Deploy default contract
    const term3 = deployTerm({
      masterRegistryUri,
      boxId: defaultBoxName,
      fungible: false,
      contractId: contractName,
      expires: undefined
    });

    const dataAtNameResponse3 = await rchainToolkit.http.easyDeploy(
      VALIDATOR_HOST,
      term3,
      privateKey,
      1,
      maxPhloLimit,
      10 * 60 * 1000
    );
  
    const data3 = rchainToolkit.utils.rhoValToJs(
      JSON.parse(dataAtNameResponse3).exprs[0].expr
    );
  
    if (data3.status !== 'completed') {
      console.info("Unable to deploy contract.");
      return;
    }
  }

  if (options.run) {
    console.info("With --run");
    fuse2 = new Fuse(mntPath, ops, { displayFolder: "RDrive", mkdir: true, debug: false });
    /*
    const onError = (err) => {
      console.error(err);
    }

    const onDone = () => {
      deployPool.terminate(); // terminate all workers when done
    }

    const testFunc = async (somemap) => {
      console.info("testFunc");
    }

    const onSuccess = (result) => {
      console.log('bundle deployed successfully!!!', result);
      deployPool.exec(testFunc, [new Map<number, DeployType[]>(deployQueue)], {workerType: 'process'})
      .then(onSuccess)
      .catch(onError)
      .then(onDone);
    }
    */

    const maxWaitTime = 5000;
    let deployBundlerInterval = undefined;

    const LoopFunc = async () => {
      //process.stdout.write('Deploying ', () => {
        
      //});
      /*
      if (deployQueue.size > 0) {
        deployPool.exec(testFunc, [new Map<number, DeployType[]>(deployQueue)], {workerType: 'process'})
        .then(onSuccess)
        .catch(onError)
        .then(onDone);
      }
      */
      
      await deployBundler(deployQueue);
      clearInterval(deployBundlerInterval);
      //console.info("clearing deployQueue");
      //deployQueue.clear();
      //.info("bundle deployed successfully");
      //flush();
      deployBundlerInterval = setInterval(LoopFunc, maxWaitTime);
    }

    deployBundlerInterval = setInterval(LoopFunc, maxWaitTime);

    //deployPool.exec(deployBundler, []);

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
}

runFunction();