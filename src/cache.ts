import path from 'path';
import { constants } from 'fs';
import Fuse from './fusen';

import { sep as pathSep } from 'path';
import {
    ensureNoTrailingSlash,
    ensureStartingSlash,
    ensureTrailingSlash,
  } from './utils';


import { createRequire } from 'module';
import { getHeapStatistics } from 'v8';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type FuseCallback = (err: number, ...args: any[]) => void;

export type StatRec = {
  mode: number; // file type and permissions
  uid: number; // user id
  gid: number; // group id
  size: number; // node content size if regular file, 4096 for directories  and symlinks
  dev: number;
  nlink: number; // number of references to the node
  ino: number; // inode number
  rdev: number; // used for device nodes
  blksize: number; // should match fs block_size
  blocks: number; // number blocks content takes, Math.ceil(size/blksize)
  atime: Date | number; // access time
  mtime: Date | number; // modification time
  ctime: Date | number; // change time of meta data
};

export interface childNode {
  name: string; // name of node in directory
  node: Node | null; // referenced node. Will only be null for .. entry in root of fs
}

export interface Node {
  stat: StatRec;
  xattr?: Map<string, Buffer>; // extended attribute storage
  children?: childNode[]; // used by directories to reference child nodes
  content?: Buffer; // used to store file contents or symlink targets
  owner?: string; // owner of node
  edit_time?: number; // last time node was edited, used for cache invalidation
  isFinalized?: boolean; // If node exists within a finalized block,
  chunksRead: number[]; // Array of 4096 chunk ids that have been read from the chain
  path: string;
}

export type IFsStats = {
  max_name_length: number;
  block_size: number;
  total_blocks: number;
  blocks_used: number;
  total_inodes: number;
  current_inode: number;
  max_fds: number;
  current_fd: number;
};

let FsStats: IFsStats = {
  max_name_length: 24, //255,
  block_size: 4096,
  total_blocks: Math.ceil(
    (getHeapStatistics().total_available_size / 4096) * 0.75
  ),
  blocks_used: 0,
  total_inodes: 100000,
  current_inode: 0,
  max_fds: 10000, // max open file descriptors
  current_fd: 0
};

/**
 * Create a stat record with default values and allow override via rec param
 * @param rec Full or partial StatRec to override defaults
 * @returns complete StatRec with defaults unless overridden by rec
 */
 export const mkStatRec = (rec?: Partial<StatRec>): StatRec => {
  // if (FsStats.current_inode === 2) FsStats.current_inode += 100;
  const ret = {
    mode: 0,
    uid: process.getuid ? process.getuid() : 0,
    gid: process.getgid ? process.getgid() : 0,
    size: 0,
    dev: 0,
    nlink: 0,
    ino: FsStats.current_inode++,
    rdev: 0,
    blksize: FsStats.block_size,
    blocks: 0,
    atime: Date.now(),
    mtime: Date.now(),
    ctime: Date.now(),
    ...rec
  };
  if (!ret.blocks) {
    ret.blocks = Math.max(0, Math.ceil((ret?.size || 0) / FsStats.block_size));
  }
  FsStats.blocks_used += ret.blocks;
  return ret;
};

// full path to node lookup
const path2Node = new Map<string, Node>();
// file descriptor to node lookup
const fd2Node = new Map<number, Node>();

/**
 * Gets node by is full path within the Fuse FS
 * @param path
 * @returns Node pointed to by path
 */
 export const getPathNode = (path: string): Node | null =>
 path2Node.get(path) || null;

/**
 * Find and return a free file descriptor for node and assign node to that fd
 * @param node
 * @returns file descriptor or -1 if one was not free
 */
 export const mkFd = (node: Node): number => {
  let f;
  let i = FsStats.max_fds; // max attempts to find free file descriptor
  do {
    f = FsStats.current_fd++;
    // wrap the counter if needed
    if (FsStats.current_fd === FsStats.max_fds) {
      FsStats.current_fd = 1;
    }
    i--;
  } while (fd2Node.has(f) && i);
  // if we did not find a free one return -1 as error code
  if (!i) {
    return Fuse.EMFILE;
  }
  // assign the node to the file descriptor
  fd2Node.set(f, node);
  // return the file descriptor
  return f;
};

export const getFsStats = (): IFsStats => FsStats;




/**
 * Create a folder named name in parentPath
 * @param parentPath Directory to create new directory in
 * @param name Name of new directory
 * @param mode file type and permission to set
 * @returns Directory node
 */
 export const mkDir = (
  parentPath: string,
  name: string,
  mode = 0o40755
): Node => {
  parentPath = ensureNoTrailingSlash(parentPath);
  const node: Node = {
    chunksRead: [],
    stat: mkStatRec({mode, nlink: 1, size: FsStats.block_size}),
    path: parentPath === "/" ? `${parentPath}${name}` : `${parentPath}/${name}`,
  };
  node.children = []; // putting this here instead of above keeps typescript happy
  node.children.push({name: '.', node: node});
  node.children.push({name: '..', node: null});
  const parentNode = getPathNode(parentPath);
  if (parentNode)
    addNodeToParent(
      parentNode,
      node,
      name,
      ensureTrailingSlash(parentPath) + name
    );
  return node;
};

/**
 * Truncate file node to specified size
 * @param node File node to truncate
 * @param size Size to truncate content to. Usually zero
 */
 export const truncateFile = async (node: Node, size = 0): Promise<void> => {
  const nodeContent = await getNodeContent(node);
  if (nodeContent) {
    await updateNodeContent(node, nodeContent.slice(0, size));
  } else {
    await updateNodeContent(node, Buffer.alloc(size));
  }
  FsStats.blocks_used -= node.stat.blocks;
  node.stat.size = size;
  node.stat.blocks = Math.ceil(size / FsStats.block_size);
  node.stat.mtime = Date.now();
  FsStats.blocks_used += node.stat.blocks;
};


/**
 * Adjust free blocks on virtual fs by numBlocks
 * @param numBlocks Number of blocks to add or subtract from total
 */
 export const adjustBlocksUsed = (numBlocks: number): number => {
  FsStats.blocks_used += numBlocks;
  FsStats.blocks_used = Math.max(
    0,
    Math.min(FsStats.blocks_used, FsStats.total_blocks)
  );
  return FsStats.total_blocks - FsStats.blocks_used;
};

/**
 * Returns last part of full path
 * @param path
 * @returns last part of full path
 */
 export const getNameFromPath = (path: string): string =>
 path.split('/').pop() || '/';

/**
 * Takes full path and returns the path portion excluding file name
 * @param path
 * @returns path sans file name. If path ends in / then returns entire path
 */
 export const getPathFromName = (path: string): string => {
  if (path.endsWith(pathSep)) {
    return path;
  }
  const p = path.split(pathSep);
  p.pop();
  path = p.join(pathSep);
  return ensureStartingSlash(path);
};

/**
 * Remove path reference to a Node
 * @param path full path within the Fuse FS
 * @returns boolean true if path is found and deleted
 */
 export const freePathNode = (path: string): boolean => path2Node.delete(path);

 


 
/**
 * Remove and unlink node from parent node
 * @param parent Parent node from which to remove node
 * @param node Node to remove
 * @param path Path mapping to remove from path to node lookup
 * @returns true if node was found and unlinked in parent otherwise false
 */
 export const removeNodeFromParent = (
  parent: Node,
  node: Node,
  path: string
): boolean => {
  freePathNode(path);
  if (!parent.children) {
    return false;
  }
  const name = getNameFromPath(path);
  let i = 0;
  for (const c of parent.children) {
    if (c.name === '.' || c.name === '..') {
      i++;
      continue;
    }
    if (c.name === name) {
      parent.children.splice(i, 1);
      if (isDirectory(node.stat.mode)) {
        parent.stat.nlink--;
      }
      node.stat.nlink--;
      if (!node.stat.nlink) {
        FsStats.blocks_used -= node.stat.blocks;
      }
      return true;
    }
    i++;
  }
  return false;
};

export const getNodeContent = async (node: Node): Promise<Buffer | undefined> => {
  //TODO: replace with a persistent in-memory cache
  return node.content;
}

export const updateNodeContent = async (
  node: Node,
  content: Buffer | string,
  updateBlocks: boolean = false,
  chunks: number[] | undefined = undefined
): Promise<number> => {
  if (!content) {
    content = Buffer.alloc(0);
  } else if (typeof content === "string") {
    content = Buffer.from(content);
  }
  node.edit_time = Date.now();

  //TODO: replace with a persistent in-memory cache
  node.content = content;

  if (chunks) {
    node.chunksRead = [...new Set([...node.chunksRead ,...chunks])];
  } else {
    const contentChunks = Math.ceil(content.length / 4096);
    node.chunksRead = [...Array(contentChunks).keys()];
  }
  if (updateBlocks) {
    node.stat.size = content.length;
    const blocks_before = node.stat.blocks;
    node.stat.blocks = Math.ceil(node.stat.size / node.stat.blksize);
    if (blocks_before !== node.stat.blocks) {
      if (adjustBlocksUsed(node.stat.blocks - blocks_before) < 1) {
        return Fuse.ENOSPC;
      }
    }
  }
  return 0;
};

export const changeNodeOwner = (
  node: Node,
  owner: string
): number => {
  node.owner = owner;
  node.edit_time = Date.now();
  return 0;
};

/**
 * Get Node referenced via file descriptor
 * @param fd the file descriptor that references a node
 * @returns Node | null if fd is not found
 */
 export const getFdNode = (fd: number): Node | null => fd2Node.get(fd) || null;

/**
 * Create file with content named name in parentPath directory
 * @param parentPath Directory to create file in
 * @param name Name of file
 * @param content Content of file
 * @param mode File type and permissions
 * @returns File node
 */
 export const mkFile = async (
  parentPath: string,
  name: string,
  content: string | Buffer,
  mode = 0o100644
): Promise<Node> => {
  parentPath = ensureNoTrailingSlash(parentPath);
  const node: Node = {
    chunksRead: [],
    stat: mkStatRec({
      mode,
      size: content.length
    }),
    path: parentPath === "/" ? `${parentPath}${name}` : `${parentPath}/${name}`,
    edit_time: Date.now()
  };
  
  await updateNodeContent(node, typeof content === 'string' ? Buffer.from(content) : content); 
  
  const parentNode = getPathNode(parentPath);
  if (parentNode) {
    addNodeToParent(
      parentNode,
      node,
      name,
      ensureTrailingSlash(parentPath) + name
    );
  } else {
    console.warn('mkFile parentNode not found');
  }
  return node;
};

export const isModeAttr = (mode: number, attr: number): boolean =>
  (mode & attr) !== 0;

export const isDirectory = (mode: number): boolean =>
  isModeAttr(mode, constants.S_IFDIR);

export const isRegularFile = (mode: number): boolean =>
  isModeAttr(mode, constants.S_IFREG);

/**
 * Adds node named name to a parentNode directory and add path2Node mapping.
 * @param parentNode Directory node to add node to
 * @param node Node to add to parentNode
 * @param name Name of the node in the parents children list
 * @param path Full path within Fuse FS that will reference the node in this parentNode
 */
 export const addNodeToParent = (
  parentNode: Node,
  node: Node,
  name: string,
  path: string
): void => {
  if (isDirectory(node.stat.mode)) {
    parentNode.stat.nlink++;
    if (!node.children) {
      node.children = [];
    }
    node.children[1].node = parentNode;
  }
  if (!parentNode.children) {
    parentNode.children = [];
  }
  node.edit_time = Date.now();
  parentNode.children.push({name: name, node});
  node.stat.nlink++;
  path2Node.set(path, node);
};

export const setPathNode = (path: string, node: Node): void =>
  path2Node.set(path, node) && undefined;
