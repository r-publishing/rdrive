/**
 * Copyright © 2019 kevinpollet <pollet.kevin@gmail.com>`
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE.md file.
 */

import fs from "fs";
import process from "process";

import path from 'path';
import Fuse from './fusen';

// import { memoryUsage } from 'node:process';

import * as rchainToolkit from "@fabcotech/rchain-toolkit";

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
  getPathFromName,
  getNameFromPath,
  removeNodeFromParent,
  adjustBlocksUsed,
  getNodeContent
} from "./cache";

import { sep as pathSep } from 'path';
import { constants } from 'fs';

import cliProgress from "cli-progress";
import byteSize from "byte-size";

import {
  masterTerm,
  deployTerm,
  readPursesTerm,
  readBoxTerm,
  readConfigTerm,
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
  swapTerm,
  creditAndSwapTerm
} from "rchain-token";

import service from "os-service";
import commandLineArgs from 'command-line-args';

import events from "events";
events.EventEmitter.defaultMaxListeners = 20000;
 
const message_file_path = path.join(import.meta.url, '../assets/message.json');
const fuse_node_path = path.join(import.meta.url, '../assets/fuse.node');
const libfuse_path = path.join(import.meta.url, '../assets/libfuse.so');


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
  SWAP,
  CREDIT_AND_SWAP
}


type DeployType = {
  id: number;
  deployType: number;
  payload: any;
  priority: number;
  revert: () => void;
  success: () => void;
};

type ExploreDeployType = {
  id: number;
  deployType: number;
  payload: any;
  priority: number;
  success: (ret: any) => void;
};

type BundledDeployType = {
  payload: any;
  termGenerator: (any) => string;
};

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
  [Deploy.SWAP, swapTerm],
  [Deploy.CREDIT_AND_SWAP, creditAndSwapTerm]
]);

//const messageFileURL = new URL("../assets/message.json", import.meta.url);
//const fileBuff = fs.readFileSync(messageFileURL.pathname);
//const { value } = JSON.parse(fileBuff.toString());

import QueuedJobs from "queued-jobs";

const readQueue = new QueuedJobs(40960, 999 * 60 * 1000);
const writeQueue = new QueuedJobs(40960, 999 * 60 * 1000);

const readBundleQueue = new QueuedJobs(40960, 999 * 60 * 1000);
const writeBundleQueue = new QueuedJobs(40960, 999 * 60 * 1000);
const maxPhloLimit = 10000000000;

//Restricts the number of data chunks deployed with each deploy.
//This is to prevent the deploy from taking too long and fit within the max deploy size.
const maxChunks = 2048;
const maxReadChunks = 4096;

//This parameter allocates x amount of workers per validator.
//If this number is too high validator can become overloaded and not respond untill all workers are finished.
const nOfWorkersPerValidator = 2;

//const pursesToRead = new Set();

const optionDefinitions = [
  { name: 'run', alias: 'r', type: Boolean },
  { name: 'clean', alias: 'c', type: Boolean },
  { name: 'masterRegUri', alias: 'u', type: String, multiple: false},
  { name: 'readOnlyHost', alias: 'o', type: String, multiple: true},
  { name: 'validatorHost', alias: 'v', type: String, multiple: true},
  { name: 'command', type: String, multiple: false, defaultOption: true },
  { name: 'privKey', alias: 'p', type: String, multiple: false },
  { name: 'boxName', alias: 'b', type: String, multiple: false },
  { name: 'contractName', alias: 'n', type: String, multiple: false },
  { name: 'mnt', alias: 'm', type: String, multiple: false },
  { name: 'shardId', alias: 's', type: String, multiple: false },
  { name: 'pursePrice', alias: 'x', type: Number, multiple: false }
]


const options = commandLineArgs(optionDefinitions)

let masterRegistryUri = options.masterRegUri;
let hasBox = false;
let hasContract = false;

let pubKey = "";
const privateKey = options.privKey;
const mntPath = options.mnt;
const defaultBoxName = options.boxName;
let contractName = options.contractName;
const shardId = options.shardId;
const pursePrice = options.pursePrice || 500000000;

const progressBars = new Map<string, cliProgress.SingleBar>();
const futureReadMap = new Map<string, number>();

const deployQueue = new Map<number, Array<DeployType>>();
const exploreDeployQueue = new Map<number, Array<ExploreDeployType>>();

function formatter(options, params, payload) {
  const bar = options.barCompleteString.substr(0, Math.round(params.progress*options.barsize)) + options.barIncompleteString.substr(0, Math.round((1-params.progress)*options.barsize));
  return 'Uploading ' + bar + ' ' + Math.floor(params.progress * 100) + "% | " + byteSize(params.value) + '/' + byteSize(params.total) + " | " + payload.name;
}

const multibar = new cliProgress.MultiBar({
  clearOnComplete: true,
  hideCursor: true,
  format: formatter
}, cliProgress.Presets.shades_grey);


readQueue.registerHandler(async (data: ExploreDeployType, requestId: number) => {
  return new Promise<any>((resolve, reject) => {
   
    data.id = requestId;

    if (!exploreDeployQueue.has(data.deployType)) {
      exploreDeployQueue.set(data.deployType, [data]);
    } else {
      exploreDeployQueue.get(data.deployType).push(data);
    }

    // @ts-ignore
    readQueue.once(`resolve:${requestId}`, (result) => {
      resolve(result)
    })
  });
});

/*
readBundleQueue.registerHandler(async (data: DeployType) => {
  if (!pursesToRead.has(data)) {
    pursesToRead.add(data);
  }
  return data;
});
*/

//=================

writeQueue.registerHandler(async (data: DeployType, requestId: number) => {

  data.id = requestId;
  if (!deployQueue.has(data.deployType)) {
    deployQueue.set(data.deployType, [data]);
  } else {
    deployQueue.get(data.deployType).push(data);
  }
})


let validatorArray = [];
if (options.validatorHost) {
  validatorArray = [...options.validatorHost];
}
const numberOfValidators = validatorArray.length;


let readOnlyArray = [];
if (options.readOnlyHost) {
  readOnlyArray = [...options.readOnlyHost];
}
const numberOfObservers = readOnlyArray.length;

for (let i = 0; i < 1; i++) {
  readBundleQueue.registerHandler(async (data: BundledDeployType) => {
    const term = data.termGenerator(data.payload);

    const ret = await rchainToolkit.http.exploreDeploy(readOnlyArray[i % numberOfObservers], {
      term: term
    }).catch(err => {
      readBundleQueue.handle(data);
    });
    
    return {data: data, ret: ret};
  });
}


let writeListeners = 0;
for (let i = 0; i < numberOfValidators * nOfWorkersPerValidator; i++) { //
  writeBundleQueue.registerHandler(async (data: BundledDeployType) => {
    const term = data.termGenerator(data.payload);

    writeListeners += 1;
    
    const ret = await rchainToolkit.http.easyDeploy(
      validatorArray[i % numberOfValidators],
      {
        term: term,
        shardId: shardId,
        privateKey: privateKey,
        phloPrice: 'auto',
        phloLimit: maxPhloLimit,
        timeout: 250 * 60 * 1000
      }
    ).catch(err => {
      writeBundleQueue.handle(data);
    });
    
    writeListeners -= 1;

    return ret;
  });
}

var logStream = fs.createWriteStream(process.argv[0] + ".log");


const exploreDeployBundler = async function(exploreDeployBundle: Map<number, Array<ExploreDeployType>>) {
  const promises = [];
  const requestIds = [];
  const successMap = new Map<number, Array<(ret: any) => void>>();
  let findDeployTypeById = Array<number>();
  
  for (const [deployType, queue] of exploreDeployBundle) {
    findDeployTypeById.push(deployType);
    const queueLength = queue.length;

    if (!successMap.has(deployType)) {
      successMap.set(deployType, []);
    }

    switch (deployType) {
      /*
      case ExploreDeploy.READ_PURSES_DATA:
        let processedChunks = [];
        let combinedSuccessMap = new Array<(ret: any) => void>();
        //Group deploys by name
        var groupedByName = queue.reduce(function(groups: Map<string, ExploreDeployType[]>, item: ExploreDeployType) {
          const purseId = item.payload.pursesIds[0];

          if (!groups.has(purseId)) {
            groups.set(purseId, []);
          }
          groups.get(purseId).push(item);
          return groups;
        }, new Map<string, ExploreDeployType[]>());

        let combinedPayload = {
          masterRegistryUri: masterRegistryUri,
          contractId: contractName,
          pursesIds: [ ...groupedByName.keys()],
          chunks: {}
        };

        for (const [purseId, queue2] of groupedByName) {
          for (const item of queue2) {
            //processedChunks.push(item.payload.chunks);
            if (!combinedPayload.chunks.hasOwnProperty(purseId)) {
              combinedPayload.chunks[purseId] = [];
            }

            combinedSuccessMap.push(item.success);
            combinedPayload.chunks[purseId] = [...new Set([...combinedPayload.chunks[purseId], ...item.payload.chunks[purseId]])];
          
            const filtered = exploreDeployBundle.get(ExploreDeploy.READ_PURSES_DATA).filter( el => el.id !== item.id);

            exploreDeployBundle.set(ExploreDeploy.READ_PURSES_DATA, filtered);
          }
        }

        //TODO
        //successMap.get(deployType).push(() => {
        //  combinedSuccessMap.map(success => success);
        //});

        if (combinedPayload.pursesIds.length > 0) {
          const termGenerator = exploreDeployTermGenerators.get(deployType);
          promises.push(readBundleQueue.handle({
            termGenerator: termGenerator,
            payload: combinedPayload
          }));

        }

        continue;
        */
      default:
        for (var i = queueLength - 1; i >= 0; i--) {
          const data = queue[i];

          try {
            const termGenerator = exploreDeployTermGenerators.get(deployType);
            promises.push(readBundleQueue.handle({
              ...data,
              termGenerator: termGenerator
            }));
            requestIds.push(data.id);
            queue.pop();
          }
          catch (err) {
            console.info(err);
            //TODO: retry?
            //queue.push(data);
          }
          
        }

    }
  }
  
  
  let ret = [];
  if (promises.length > 0) {
    ret = await Promise.all(promises);
  }
  else {
    return;
  }

  ret.forEach((deployRet: {data: ExploreDeployType, ret: any}, i) => {
    const deployType = findDeployTypeById[i];

    if (!deployType) {
      return;
    }
    

    switch (deployType) {
      //case ExploreDeploy.READ_PURSES_DATA:
        /*
        if (data.status === 'completed') {
          //console.info("Box deployed!");
          deployBundle.delete(deployType);
        }

        return deployBundle;
        */
       //return ret;

      default:
        if (deployRet.data.success) {
          deployRet.data.success(deployRet.ret);
        }

        exploreDeployBundle.delete(deployType);
        // @ts-ignore
        readQueue.dispatchEvent(`resolve:${deployRet.data.id.toString()}`, deployRet.ret);
    }

    exploreDeployBundle.delete(deployType);
  });

  return exploreDeployBundle;
}

//Don't ask me how it works, it just does, ok?
const deployBundler = async function(deployBundle: Map<number, Array<DeployType>>) {
  const promises = [];
  const reverts = new Map<number, Array<() => any>>();
  const successMap = new Map<number, Array<() => any>>();
  const deploys = new Map<number, DeployType>();
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

    if (queueLength == 0) {
      continue;
    }

    switch (deployType) {
      //Here we bundle all CREATE_PURSES deploys into one + any other deploys like UPDATE_PURSE_DATA.
      case Deploy.CREATE_PURSES:
        let payload = {
          masterRegistryUri,
          contractId: contractName,
          purses: {},
          data: {}
        };

        let combinedSuccessMap = new Array<() => any>();
        let combinedRevertsMap = new Array<() => any>();

        let combinedSuccessMap2 = new Array<() => any>();
        let combinedRevertsMap2 = new Array<() => any>();

        let chunksTotal = 0;
        for (var i = 0; i < queueLength; i++) {
          const data = queue[i];
          
          //TODO: take maxNumberOfChunks into account
          if (chunksTotal + Object.keys(data.payload.data).length > maxChunks) {
            break;
          }

          payload.purses = {...payload.purses, ...data.payload.purses};
          payload.data = {...payload.data, ...data.payload.data};

          combinedSuccessMap.push(data.success);
          combinedRevertsMap.push(data.revert);

          deploys.set(data.id, data);
          chunksTotal += Object.keys(data.payload.data).length;
        }

        if (Object.keys(payload.purses).length == 0) {
            continue;
        }

        let bundled = [];
        
        if (deployBundle.has(Deploy.UPDATE_PURSE_DATA)) {
          const dataQueue = deployBundle.get(Deploy.UPDATE_PURSE_DATA);
          for (var i = 0; i < dataQueue.length; i++) {
            const dataDeploy = dataQueue[i];

            const chunkId = dataDeploy.payload.pos / 4096;

            if (!Object.keys(payload.purses).includes(dataDeploy.payload.purseId)) {
              continue;
            }

            //deploy would be too large
            if (payload.data[dataDeploy.payload.purseId] && Object.keys(payload.data[dataDeploy.payload.purseId]).length > maxChunks ) {
              break
            }

            payload.data[dataDeploy.payload.purseId] = {...payload.data[dataDeploy.payload.purseId], [chunkId]: dataDeploy.payload.data};
            
            combinedSuccessMap2.push(dataDeploy.success);
            combinedRevertsMap2.push(dataDeploy.revert);

            deploys.set(dataDeploy.id, dataDeploy);

            const filtered = deployBundle.get(Deploy.UPDATE_PURSE_DATA).filter( el => el.id !== dataDeploy.id);

            deployBundle.set(Deploy.UPDATE_PURSE_DATA, filtered);

            bundled.push(chunkId);
          }
        }
        /*
        if (deployBundle.has(Deploy.UPDATE_PURSE_PRICE)) {
          const priceQueue = deployBundle.get(Deploy.UPDATE_PURSE_PRICE);

          for (var i = priceQueue.length - 1; i >= 0; i--) {
            const priceDeploy = priceQueue[i];

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

        payload.data = Object.fromEntries(Object.entries(payload.data).map(([k,v], i) => [k, Object.values(v)])) ;

        try {
          
          if (!successMap.has(Deploy.UPDATE_PURSE_DATA)) {
            successMap.set(Deploy.UPDATE_PURSE_DATA, []);
          }

          if (!reverts.has(Deploy.UPDATE_PURSE_DATA)) {
            reverts.set(Deploy.UPDATE_PURSE_DATA, []);
          }
          

          reverts.get(deployType).push(() => {
            combinedRevertsMap.map(revert => revert());
          });
          successMap.get(deployType).push(() => {
            combinedSuccessMap.map(success => success());
          });
          
          reverts.get(Deploy.UPDATE_PURSE_DATA).push(() => {
            combinedRevertsMap2.map(revert => revert());
          });
          successMap.get(Deploy.UPDATE_PURSE_DATA).push(() => {
            combinedSuccessMap2.map(success => success());
          });
          

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
          let processedChunks = [];
          //Group deploys by name
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
                data: {},
                pos: 0,
              };

              let combinedSuccessMap = new Array<() => any>();
              let combinedRevertsMap = new Array<() => any>();

              for (var i = 0; i < queueLength3; i++) {
                const data = queue3[i];

                const chunkId = data.payload.pos / 4096;

                //deploy would be too large
                if (Object.keys(payload3.data).length > maxChunks ) {
                  break
                }

                payload3.data = {...payload3.data, [chunkId]: data.payload.data};
                
                combinedRevertsMap.push(data.revert);
                combinedSuccessMap.push(data.success);

                deploys.set(data.id, data);
                const filtered = deployBundle.get(deployType).filter( el => el.id !== data.id);

                deployBundle.set(deployType, filtered);

                processedChunks.push(chunkId);
              }

              const pos = parseInt(Object.keys(payload3.data)[0]);
              payload3.data = JSON.stringify(Object.values(payload3.data));
              payload3.pos = pos;

              const termGenerator = deployTermGenerators.get(deployType);

              
              reverts.get(deployType).push(() => {
                combinedRevertsMap.map(revert => revert());
              });
              successMap.get(deployType).push(() => {
                combinedSuccessMap.map(success => success());
              });

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
          const data = queue[i];
          if (deploys.has(data.id)) {
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
            queue.pop();
          }
          catch (err) {
            console.info(err);
            //TODO: retry?
            //queue.push(data);
          }
        }
        deployBundle.delete(deployType);

    }


  }

  let ret = [];
  if (promises.length > 0) {
    ret = await Promise.all(promises);
  }
  else {
    return deployBundle;
  }

  ret.forEach((deployRet, i) => {
    const deployType = findDeployTypeById[i];

    if (!deployType) {
      return deployBundle;
    }

    const data = rchainToolkit.utils.rhoValToJs(
      JSON.parse(deployRet).exprs[0].expr
    );

    switch (deployType) {
      case Deploy.DEPLOY_BOX:
        if (data.status === 'completed') {
          deployBundle.delete(deployType);
        }
        else {
          reverts.get(deployType)[i]();
        }

        return deployBundle;
      case Deploy.CREATE_PURSES:
        if (data.status === 'completed' && data.results) {
          for (const [uniqueId, res] of Object.entries(data.results)) {
            if (res === false) {
              const deploy = deploys.get(parseInt(uniqueId));
              deploy.revert();
              const deploy2 = deploys.get(parseInt(uniqueId));
              deploy2.revert();
            } else {
              successMap.get(deployType)[i](); //TODO: check
              successMap.get(Deploy.UPDATE_PURSE_DATA)[i]();
            }
          }

          deployBundle.delete(deployType);
        }
        else {
          //reverts.get(deployType)[i]();
        }

        return deployBundle;
      case Deploy.DELETE_PURSE:
        if (data.status === 'completed') {
          //successMap.get(deployType)[i]();
          deployBundle.delete(deployType);
        }
        else {
          reverts.get(deployType)[i]();
        }

        return deployBundle;
      case Deploy.CREDIT:

        if (data.status === 'completed') {
          //successMap.get(deployType)[i]();
          deployBundle.delete(deployType);
        }
        else {
          reverts.get(deployType)[i]();
        }

        return deployBundle;
      case Deploy.UPDATE_PURSE_DATA:
        if (data.status === 'completed') {
          successMap.get(deployType)[i]();
          //deployBundle.delete(deployType);
        }
        else {
          //reverts.get(deployType)[i]();
        }

        return deployBundle;
      case Deploy.UPDATE_PURSE_PRICE:
        if (data.status === 'completed') {
          //successMap.get(deployType)[i]();
          deployBundle.delete(deployType);
        }
        else {
          reverts.get(deployType)[i]();
        }

        return deployBundle;
      case Deploy.WITHDRAW:

        if (data.status === 'completed') {
          //successMap.get(deployType)[i]();
          deployBundle.delete(deployType);
        }
        else {
          reverts.get(deployType)[i]();
        }

        return deployBundle;
      case Deploy.SWAP:

        if (data.status === 'completed') {
          //successMap.get(deployType)[i]();
          deployBundle.delete(deployType);
        }
        else {
          //reverts.get(deployType)[i]();
        }

        return deployBundle;
        case Deploy.CREDIT_AND_SWAP:

          if (data.status === 'completed') {
            //successMap.get(deployType)[i]();
            deployBundle.delete(deployType);
          }
          else {
            reverts.get(deployType)[i]();
          }
  
          return deployBundle;
      default:
        //console.info("unknown deploy deployType ", deployType);
        return deployBundle
    }
  })

  return deployBundle;
  
}


const bulkExploreDeploy = async function (deployType: ExploreDeploy, payload: any, success?: (ret: any) => any, priority?: number) {
  const result = await readQueue.handle({
    deployType: deployType,
    payload: payload,
    priority: priority || 0,
    success: success
  });
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
}

const FetchChunks = (node: Node, pos: number, len: number, purseName: string, aheadRead2: number, chunksToRead: number[], aheadChunks: number[]) : Promise<boolean> => {
  return new Promise<any>(async (resolve, reject) => {
    await bulkExploreDeploy(ExploreDeploy.READ_PURSES_DATA, {
      masterRegistryUri: masterRegistryUri,
      pursesIds: [purseName],
      contractId: contractName,
      chunks: {
        [purseName] : [...chunksToRead, ...aheadChunks]
      }
    }, async (result) => {
      const parsedResult = JSON.parse(result);

      //TODO: Shouldn't be necessary if data is intact, but just in case handle errors
      //if (typeof parsedResult === 'string' && parsedResult.startsWith("Unexpected error")) {
        //console.info("Found err");
        //resolve(false);
      //}

      const retData = rchainToolkit.utils.rhoValToJs(parsedResult.expr[0]);
      
      if (retData[purseName]) {
        /*
        let totalLength = 0;

        Object.keys(retData[purseName]).forEach((chunkId: string) => {
          const data = retData[purseName][chunkId];
          const chunkContent = Buffer.from(data, 'base64url');
          totalLength += chunkContent.length;
        });
        */

        var b = Buffer.alloc(len + aheadRead2 * 4096);
        /* //TODO.....
        Object.keys(retData[purseName]).forEach((chunkId: string) => {
          const data = retData[purseName][chunkId];
          const dataAsStr = Buffer.from(data, "base64url");
          const chunkb = Buffer.from(data.toString("utf8"), 'base64url');
          chunkb.copy(b, (parseInt(chunkId) - chunkStart) * 4096, 0, dataAsStr.length);
        });
        */
        retData[purseName].forEach((chunk: string, chunkId: number) => {
          const data = chunk;
          const dataAsStr = Buffer.from(data, "base64url");
          const chunkb = Buffer.from( data, 'base64url');
          chunkb.copy(b, chunkId * 4096, 0, dataAsStr.length);
        });

        const chunkData2 = b.slice(0, len + aheadRead2 * 4096);
        const nodeContent = await getNodeContent(node);
        const fillData = Buffer.alloc(Math.max(0, pos - nodeContent.length));
        const chunkDataRest = nodeContent.slice(pos + chunkData2.length, nodeContent.length);
        const str2 = Buffer.concat([
          nodeContent.slice(0, pos),
          fillData,
          chunkData2,
          chunkDataRest
        ]);

        await updateNodeContent(node, str2, false, [...chunksToRead, ...aheadChunks]);

        resolve(true);
      }

    });
    resolve(true);
  });
}

const ops = {
  init: async function(cb: FuseCallback) {
    const rootNode = mkDir("", "");
    setPathNode("", rootNode);
    setPathNode(pathSep, rootNode);

    await mkFile(pathSep, ".token.conf", "{\n \"price\": " +pursePrice+ "\n}\n");

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

    if (inPath !== '/') {
      const boxName = inPath.split("/")[1];
      const purseName = path.basename(inPath);

      const result = await bulkExploreDeploy(ExploreDeploy.READ_BOX, { masterRegistryUri: masterRegistryUri, boxId: boxName})

      const boxResult = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);

      if (boxResult && boxResult.purses) {
        const purses = boxResult.purses[contractName];
        const revPurses = boxResult.purses["rev"];

        if (revPurses && revPurses.length > 0) {

          const result = await bulkExploreDeploy(ExploreDeploy.READ_PURSES, {
            masterRegistryUri: masterRegistryUri,
            contractId: "rev",
            pursesIds: revPurses,
          });


          const purses = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);
          const balance = purses[revPurses[0]].quantity;


          const node = getPathNode(pathSep + boxName + pathSep + "revbalance.rev");

          if (!node) {
            await mkFile(pathSep + boxName, "revbalance.rev", balance.toString() + "\n");
          }
          else {
            //Update balance
            await updateNodeContent(node, balance.toString() + "\n");
          }
        }
        if (purses && purses.length > 0) {
          const pursesResult = await bulkExploreDeploy(ExploreDeploy.READ_PURSES, {
            masterRegistryUri: masterRegistryUri,
            contractId: contractName,
            pursesIds: purses,
          });
          const pursesInfo = rchainToolkit.utils.rhoValToJs(JSON.parse(pursesResult).expr[0]);

          purses.forEach(async (purse: string) => {
            const purseInfo = pursesInfo[purse];
            const node = getPathNode(pathSep + boxName + pathSep + purse);
            
            if (!node) {
              const fileNode = await mkFile(pathSep + boxName, purse, "");
              fileNode.isFinalized = true;

              const nOfChunks = purseInfo.chunks  || 1;
              const prevBlocksSize = fileNode.stat.blocks;
              fileNode.stat.size = nOfChunks * 4096;

              fileNode.stat.blksize = 512; //For some reason necessary for Fuse to report correct file size.
              fileNode.stat.blocks = nOfChunks * 8; //How many blocks it would have been if they were of size 512
              
              //fileNode.content = Buffer.alloc(fileNode.stat.size);
              if (prevBlocksSize !== fileNode.stat.blocks) {
                adjustBlocksUsed(fileNode.stat.blocks - prevBlocksSize);
              }

              const f = mkFd(fileNode);
            }

            if (purseInfo && purseInfo.hasOwnProperty("price") && purseInfo.price) {
              
              const node2 = getPathNode(pathSep + boxName + pathSep + "." + purse + "." + contractName);
              if (!node2) {
                const fileConfigNode = await mkFile(pathSep + boxName, "." + purse + "." + contractName, `{\n price: ${purseInfo.price[1]}\n}\n`);
                mkFd(fileConfigNode);
              }
              else {
                await updateNodeContent(node2, `{\n \"price\": ${purseInfo.price[1]}\n}\n`);
              }
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
      //console.info("Can't create purse in a box that is not yours!");
      return process.nextTick(cb, Fuse.EACCES);
    }

    const contractConfigFileNode = getPathNode(pathSep + ".token.conf");
    let price = 0;
    const nodeContent = await getNodeContent(contractConfigFileNode);
    if (contractConfigFileNode && nodeContent) {
      const config = JSON.parse(nodeContent.toString());
      price = parseInt(config.price);
    }

    const extension = path.extname(name);
    if (!isHiddenFile && extension !== ".rev") {
      const payload = {
        masterRegistryUri,
        contractId: contractName,
        purses: {
          [name]: {
            id: name,
            price: `("rev", ${price})`,
            boxId: boxId,
            quantity: 1,
          },
        },
        data: {} 
      }

      let fileConfigNode = getPathNode(inPath);
      if (!fileConfigNode) {
        const fileConfigNode = await mkFile(p.join(pathSep), "." + name + "." + contractName, "{\n \"price\": " + price + "\n}\n", mode);
        mkFd(fileConfigNode);
      }
  
      const progressBar = multibar.create(1, 0, {
        name: inPath
      }, {clearOnComplete: true});
      progressBars.set(inPath, progressBar);
      //flush();

      bulkDeploy(Deploy.CREATE_PURSES, payload, () => {
        //Revert on failure
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
      });
    }


    if (isRegularFile(mode)) {
      node = await mkFile(p.join(pathSep), name, "", mode);
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
    return process.nextTick(cb, 0);
  },
  getattr: async function (inPath: string, cb: FuseCallback) {
    const node = getPathNode(inPath);
    if (!node) {
      return process.nextTick(cb, Fuse.ENOENT);
    }

    return process.nextTick(cb, 0, node.stat);
  },
  fgetattr: function (fd: number, cb: FuseCallback) {
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
    const extension = path.extname(purseName);
    const node = getPathNode(inPath);
    const isSpecialFile = extension === ".rev" || purseName.startsWith(".");

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

    const nodeContent = await getNodeContent(node);
    if (nodeContent) {
      const localContent = nodeContent.slice(pos, pos + len);
      const blocks = node.stat.blocks / 8;
      const lastChunk = (pos + len) / 4096;

      const chunksToRead = [...Array(chunksLength).keys()].map(i => i + chunkStart).filter(i => i < blocks);
      const remoteChunks = chunksToRead.filter(x => !node.chunksRead.includes(x)); //Chunks that don't exist in cache yet.

      //There is no need to read from chain if we have all the chunks in memory, but we still want to continue reading ahead in reasonable amounts.
      if (remoteChunks.length == 0 && !isSpecialFile) {
          const futureRead = [...Array(maxReadChunks).keys()].map(i => i + chunkStart).filter(i => i < blocks && !node.chunksRead.includes(i));
          const futureReadDiff = futureReadMap.get(inPath) == futureRead.length;
          futureReadMap.set(inPath, futureRead.length);
          
          //Predict read ahead and fetch chunks before we run out of cache
          //Read at least 1024 chunks ahead, or what's remaining.
          if (futureRead.length > 1024 || (futureRead.length > 0 && futureReadDiff)) {
            node.chunksRead = [...new Set([...node.chunksRead ,...futureRead])]; //TODO: revert if not read;
            FetchChunks(node, futureRead[0] * 4096, futureRead.length, purseName, futureRead.length, [], futureRead);
          }
      }
      
      //Some chunks need to be read immediately.
      if (remoteChunks.length > 0 && !isSpecialFile) {
        const aheadRead = maxReadChunks - chunksToRead.length;

        let aheadChunks = [];
        
        if (chunksToRead.length < maxReadChunks && (chunksToRead[0] + chunksToRead.length) < blocks ) {
          aheadChunks = [...Array(aheadRead).keys()].map(i => i + chunksToRead[chunksToRead.length - 1] + 1).filter(i => i < blocks);
        }
        let aheadRead2 = aheadChunks.length;
        
        if (lastChunk <= blocks && inPath.split("/").length > 2 && chunksToRead.length > 0) {
          await FetchChunks(node, pos, len, purseName, aheadRead2, chunksToRead, aheadChunks);

          const nodeContent2 = await getNodeContent(node);
          const remoteContent = nodeContent2.slice(pos, pos + len);
          
          remoteContent.copy(buf);
          return process.nextTick(cb, len);
        }
        else {
          return process.nextTick(cb, len);
        }
        
      }
      else {
        //Continue to read from cache
        localContent.copy(buf);
        return process.nextTick(cb, len);
      }
    }

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
    const fileName = path.basename(inPath);
    const extension = path.extname(fileName);
    const isHiddenFile = fileName.startsWith(".");
    const isSpecialFile = extension === ".rev" || extension === "."+contractName;

    if (inPath === "." || inPath === "..") {
      return process.nextTick(cb, Fuse.EINVAL);
    }
    const node = getPathNode(inPath);
    if (!node) {
      return process.nextTick(cb, Fuse.ENOENT);
    }

    if (!isHiddenFile && !isSpecialFile) {
      const boxName = inPath.split("/")[1];
      const purseName = path.basename(inPath);
      let payload = {
        masterRegistryUri: masterRegistryUri,
        contractId: contractName,
        purseId: purseName,
      };
      bulkDeploy(Deploy.DELETE_PURSE, payload);
    }

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

    const nodeContent = await getNodeContent(node);
    if (pos) {
      content = Buffer.concat([
        nodeContent.slice(0, pos),
        buf.slice(0, length)
      ]);
    } else {
      content = Buffer.from(buf.slice(0, length));
    }
    const ret = await updateNodeContent(node, content);

    const boxNode = getPathNode(pathSep + boxName);
    if (boxNode && !boxNode.owner && isDirectory(boxNode.stat.mode)) {
      //TODO: Memorize this
      /*
      const result = await bulkExploreDeploy(ExploreDeploy.READ_BOX, { masterRegistryUri: masterRegistryUri, boxId: boxName});
      const boxResult = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);

      if (boxResult && boxResult.publicKey !== pubKey) {
        //console.info("Can't update data in a box that is not yours!");
        return process.nextTick(cb, Fuse.EACCES);
      }

      if (boxResult) {
        changeNodeOwner(boxNode, boxName)
      }
      */
    }


    if (!isHiddenFile) {
      if (extension === ".rev") {
        const result = await bulkExploreDeploy(ExploreDeploy.READ_BOX, { masterRegistryUri: masterRegistryUri, boxId: boxName});
        const boxResult = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);
  
        if (boxResult && boxResult.publicKey !== pubKey) {

          return process.nextTick(cb, Fuse.EACCES);
        }
        
        if (boxResult && boxResult.purses) {
          const purses = boxResult.purses[contractName];
          const revPurses = boxResult.purses["rev"];

          let currentBalance = 0;

          if (revPurses && revPurses.length > 0) {
            //Has REV
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
            //TOP UP
            const payload = {
              revAddress: rchainToolkit.utils.revAddressFromPublicKey(pubKey),
              quantity: topUp,
              masterRegistryUri: masterRegistryUri,
              boxId: boxName
            }
            bulkDeploy(Deploy.CREDIT, payload, () => {}, boxName, () => {});
          }
          if (topUp < 0) {
            //Do withdraw
            const payload = {
              masterRegistryUri: masterRegistryUri,
              withdrawQuantity: Math.abs(topUp),
              purseId: revPurses[0],
              toBoxId: "_rev",
              boxId: boxName,
              contractId: "rev",
              merge: false,
            };
            bulkDeploy(Deploy.WITHDRAW, payload, () => {}, boxName, () => {});
          }
        }

      } else {
        const payload = {
          masterRegistryUri: masterRegistryUri,
          purseId: purseName,
          boxId: boxName,
          contractId: contractName,
          data: Buffer.from(buf.slice(0, length), "utf8").toString("base64url"), //Buffer.from(content),
          length: length,
          pos: pos,
        };
          const progress = progressBars.get(inPath);
          progress.setTotal(pos + length);

          bulkDeploy(Deploy.UPDATE_PURSE_DATA, payload, () => {}, purseName+"@"+boxName, () => {
            progress.increment(length);
          });
      }
    }
    else {
        //Update purse config
        if (content) {
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
          bulkDeploy(Deploy.UPDATE_PURSE_PRICE, payload, () => {}, purseId, () => {
          });
        }
    }
    
    
    const nOfBlocks = Math.ceil(length / 4096);
    const prevBlocksSize = node.stat.blocks;
    node.stat.size += length;
    node.stat.blocks += nOfBlocks * 8;
    node.stat.blksize = 512;
    
    if (prevBlocksSize !== node.stat.blocks) {
      adjustBlocksUsed(node.stat.blocks - prevBlocksSize);
    }
    
    return process.nextTick(cb, ret < 0 ? ret : length);
  },
  rename: async function(src: string, dst: string, cb: FuseCallback) {
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
            bulkDeploy(Deploy.SWAP, payload, () => {}, purseName, () => {});

        } else {
            let quantity = 1;
            let purseId = purseName;
            let currentBalance = 0;
            const srcNodeContent = await getNodeContent(srcNode);
            if (extension === ".rev" && srcNodeContent) {
              quantity = parseInt(srcNodeContent.toString());

              const result = await bulkExploreDeploy(ExploreDeploy.READ_BOX, { masterRegistryUri: masterRegistryUri, boxId: srcBoxName});
              const boxResult = rchainToolkit.utils.rhoValToJs(JSON.parse(result).expr[0]);
        
              if (boxResult && boxResult.publicKey !== pubKey) {
                //console.info("Can't move rev from a purse that isn't yours!");
                return process.nextTick(cb, Fuse.EACCES);
              }
              
              if (boxResult && boxResult.purses && boxResult.purses.hasOwnProperty("rev")) {
                //const purses = boxResult.purses[contractName];
                const revPurses = boxResult.purses["rev"];
                purseId = revPurses[0];

                if (revPurses && revPurses.length > 0) {
                  //Has REV
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
              bulkDeploy(Deploy.WITHDRAW, payload, async () => {
                //console.info("transaction unsuccessful! :(");
                //Revert to previous balance
                await mkFile(src, purseName, currentBalance.toString());
              }, purseName, () => {
                //console.info("transaction successful! :)");
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
    let node: Node | null = null;
    const p = inPath.split(pathSep);
    const name = p.pop();
    const isHiddenFile = name.startsWith(".");
    
    if (p.length > 1) {
      return process.nextTick(cb, Fuse.ENOENT);
    }

    if (!name || isHiddenFile) {
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
      bulkDeploy(Deploy.DEPLOY_BOX, payload, () => {}, name, () => {});
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
  console.clear();
}

const entryFunc = async function () {
  flush();
  pubKey = rchainToolkit.utils.publicKeyFromPrivateKey(options.privKey);

  if (!masterRegistryUri) {
    console.info("No master registry uri, creating a new one. Please wait...");
    const term = masterTerm({
      depth: 3,
      contractDepth: 2,
    });
    let dataAtNameResponse;
    try {
      dataAtNameResponse = await rchainToolkit.http.easyDeploy(
        validatorArray[0 % numberOfValidators],
        {
          term: term,
          shardId: shardId,
          privateKey: privateKey,
          phloPrice: 'auto',
          phloLimit: maxPhloLimit,
          timeout: 250 * 60 * 1000
        }
      );
    }
    catch (err) {
      //console.info(err);
    }
  
    const data = rchainToolkit.utils.rhoValToJs(
      JSON.parse(dataAtNameResponse).exprs[0].expr
    );

    masterRegistryUri = data.registryUri.replace('rho:id:', '');
  }

  console.info("RDrive ready (with uri: " + masterRegistryUri + ")");

  const term = readBoxTerm({ masterRegistryUri: masterRegistryUri, boxId: defaultBoxName})
  const boxResult = await rchainToolkit.http.exploreDeploy(readOnlyArray[0], {
    term: term,
  });

  const data = rchainToolkit.utils.rhoValToJs(JSON.parse(boxResult).expr[0]);

  if (typeof data === "string" && data.startsWith("error")) {
    //Deploy box
    console.info("Deploying new box...");
    const term2 = deployBoxTerm({
      masterRegistryUri: masterRegistryUri,
      boxId: defaultBoxName,
      publicKey: pubKey,
      revAddress: rchainToolkit.utils.revAddressFromPublicKey(pubKey),
    });

    let dataAtNameResponse2;
    try {
      dataAtNameResponse2 = await rchainToolkit.http.easyDeploy(
        validatorArray[0 % numberOfValidators],
        {
          term: term2,
          shardId: shardId,
          privateKey: privateKey,
          phloPrice: 'auto',
          phloLimit: maxPhloLimit,
          timeout: 250 * 60 * 1000
        }
      );
    }
    catch (err) {
      console.info(err);
    }
  
    const data2 = rchainToolkit.utils.rhoValToJs(
      JSON.parse(dataAtNameResponse2).exprs[0].expr
    );
  
    if (data2.status === 'completed') {
      console.info("Folder " + defaultBoxName + " created.");
      hasBox = true;
    }
  } else {
    hasBox = true;
  }

  if (contractName) {
    const term3 = readConfigTerm({ masterRegistryUri: masterRegistryUri, contractId: contractName})

    const contractResult = await rchainToolkit.http.exploreDeploy(readOnlyArray[0], {
      term: term3,
    });


    if (JSON.parse(contractResult).expr && JSON.parse(contractResult).expr.length > 0) {
      //const contractData = rchainToolkit.utils.rhoValToJs(JSON.parse(contractResult).expr[0]);
      hasContract = true;
    }
  }
  else {
    contractName = "default";
  }
  
  if (hasBox && !hasContract) {
    //Deploy personal contract
    console.info("Deploying contract...");
    const term3 = deployTerm({
      masterRegistryUri,
      boxId: defaultBoxName,
      fungible: false,
      contractId: contractName,
      expires: undefined
    });

    let dataAtNameResponse3;
    try {
      dataAtNameResponse3 = await rchainToolkit.http.easyDeploy(
        validatorArray[0 % numberOfValidators],
        {
          term: term3,
          shardId: shardId,
          privateKey: privateKey,
          phloPrice: 'auto',
          phloLimit: maxPhloLimit,
          timeout: 250 * 60 * 1000
        }
      );
    }
    catch (err) {
      console.info(err);
    }

    const data3 = rchainToolkit.utils.rhoValToJs(
      JSON.parse(dataAtNameResponse3).exprs[0].expr
    );
  
    if (data3.status !== 'completed') {
      //console.info("Unable to deploy contract.");
      return;
    }

    flush();
    console.info("RDrive ready (with uri: " + masterRegistryUri + ")");
  }

  if (options.run) {
    fuse2 = new Fuse(mntPath, ops, { displayFolder: "RDrive", mkdir: true, debug: false });

    let maxWaitTime = 10000;
    let deployBundlerInterval = undefined;
    let processingLag = 0;
    const LoopFunc = async () => {
      //flush();
      //console.info("RDrive ready (with uri: " + masterRegistryUri + ")");

      //console.log(memoryUsage());


      //Take a snapshot of the deploy queue, bundle as much as possible, and deploy it
      const toProcess: Map<number, DeployType[]> = new Map<number, DeployType[]>();
      
      for (const [deployType, queue] of deployQueue) {
        toProcess.set(deployType, [...queue]);
        deployQueue.get(deployType).length = 0;
        deployQueue.delete(deployType);
      }
      if (toProcess.has(Deploy.UPDATE_PURSE_DATA)) {
        processingLag = maxChunks - Math.round((processingLag + Math.min(maxChunks, toProcess.get(Deploy.UPDATE_PURSE_DATA).length)) / 2);
        maxWaitTime = processingLag * (maxChunks / 32);
      }
      
      
      const deployBundleRemaining = await deployBundler(toProcess);

      //Put deploys that weren't bundled back into the queue
      for (const [deployType, queue] of deployBundleRemaining) {
        if (!deployQueue.get(deployType)) {
          deployQueue.set(deployType, []);
        }
        deployQueue.set(deployType, [...queue, ...deployQueue.get(deployType)]);
      }
      

      clearInterval(deployBundlerInterval);

      deployBundlerInterval = setInterval(LoopFunc, maxWaitTime);
    }

    deployBundlerInterval = setInterval(LoopFunc, maxWaitTime);
    
    
    const maxExploreWaitTime = 500;
    let exploreDeployBundlerInterval = undefined;
    
    const ExploreLoopFunc = async () => {
      /*
      const toProcess: Map<number, ExploreDeployType[]> = new Map<number, ExploreDeployType[]>();
      for (const [deployType, queue] of exploreDeployQueue) {
        toProcess.set(deployType, [...queue]);
        exploreDeployQueue.get(deployType).length = 0;
        exploreDeployQueue.delete(deployType);
      }

      const exploreDeployBundleRemaining = */ await exploreDeployBundler(exploreDeployQueue);
      /*
      for (const [deployType, queue] of exploreDeployBundleRemaining) {
        exploreDeployQueue.set(deployType, [...queue, ...exploreDeployQueue.get(deployType)]);
      }
      */
      
      
      clearInterval(exploreDeployBundlerInterval);

      exploreDeployBundlerInterval = setInterval(ExploreLoopFunc, maxExploreWaitTime);
    }

    exploreDeployBundlerInterval = setInterval(ExploreLoopFunc, maxExploreWaitTime);

    service.run (function () {
      fuse2?.unmount( async () => {
        logStream.write("Unmounted" + "\n");
        //TODO: Wait until all deploys are sent
        setTimeout(() => {
          clearInterval(deployBundlerInterval);
          clearInterval(exploreDeployBundlerInterval);
          service.stop(0);
        }, 2000);
      });
    });

    
    logStream.write("Running service" + "\n");
    fuse2.mount(function (err: any) {
      logStream.write("Mounted" + "\n");
    })
    

    process.on('SIGINT', async () => {
        //console.info("Unmounting...");
        fuse2?.unmount( () => {
          logStream.write("Unmounted" + "\n");
          //TODO: Wait until all deploys are sent
          setTimeout(() => {
            clearInterval(deployBundlerInterval);
            clearInterval(exploreDeployBundlerInterval);
            service.stop(0);
            process.exit(0);
          }, 2000);
        });
    });
  }
  else if (options.clean) {
    if (fuse2) {
      fuse2.unmount( () => {
        logStream.write("Unmounted" + "\n");
      });
    }
  }
  else {
    console.info("Usage: rdrive-service up|down");
  }
}

entryFunc();