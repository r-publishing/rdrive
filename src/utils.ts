import { sep as pathSep } from 'path';
/**
 * Match all occurrences of  // or \\  in strings with dependence on pathSep for OS
 */
const doubleSlashMatcher = new RegExp(
    pathSep.repeat(pathSep === '\\' ? 4 : 2),
    'g'
);

/**
 * Remove double slashes from a path
 * @param path to remove double slashes from
 * @returns path without double slashes
 */
export const removeDoubleSlashes = (path: string): string =>
    path.replace(doubleSlashMatcher, pathSep);

/**
 * Ensure path starts with slash
 * @param path to ensure starts with slash
 * @returns path with starting slash
 */
export const ensureStartingSlash = (path: string): string =>
    path.startsWith(pathSep) ? path : pathSep + path;

/**
 * Ensure path ends with slash
 * @param path to ensure ends with a slash
 * @returns path with trailing slash
 */
export const ensureTrailingSlash = (path: string): string =>
    removeDoubleSlashes(
        ensureStartingSlash(path.endsWith(pathSep) ? path : path + pathSep)
    );

/**
 * Ensure path does not end with slash
 * @param path to ensure does not end with slash
 * @returns path without trailing slash
 */
export const ensureNoTrailingSlash = (path: string): string =>
    removeDoubleSlashes(
        ensureStartingSlash(path.endsWith(pathSep) ? path.slice(0, -1) : path)
    );