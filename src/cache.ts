/* SPDX-License-Identifier: BSD-3-Clause */
/* Copyright (c) 2026 Bjoern Boss Henrichsen */
import * as libLog from "./log.js";
import * as libHelper from "./helper.js";
import * as libBase from "./base.js";
import * as libFs from "fs";
import * as libFsPromises from "fs/promises";
import * as libStream from "stream";
import * as libCrypto from "crypto";

const UNIQUE_ID_CHARS = '0123456789abcdefghijklmnopqrstuvwxyz'
const UNIQUE_ID_LENGTH = 10;
const ID_EXTENSION_REGEX = RegExp(`^\\.[${UNIQUE_ID_CHARS}]{${UNIQUE_ID_LENGTH}}$`, 'i');
const FileReadableBrand = Symbol('mws.file-readable');
let nextTemporaryId = 0;

function readStats(path: string): null | [number, number] {
	try {
		const stats = libFs.statSync(path);
		if (stats.isFile())
			return [stats.size, stats.mtimeMs];
	} catch (err: any) {
		if (err.code == 'ENOENT')
			return null;
		throw err;
	}
	return null;
}
function wrapError(err: any, message: string): NodeJS.ErrnoException {
	/* rewrap the error to attach context to the message, but preserve the system
	*	error code and keep the original error accessible via the cause */
	const wrapped: NodeJS.ErrnoException = new Error(`${message}: ${err.message}`, { cause: err });
	if (typeof err?.code == 'string')
		wrapped.code = err.code;
	return wrapped;
}
function sniffStream(stream: libStream.Readable, expected: number | null, callback: (data: Buffer) => void): libStream.Readable {
	let buffers: Buffer[] = [], settled: boolean = false, totalLength: number = 0;

	/* create the transformer stream to cache the data */
	const sniffer = new libStream.Transform({
		transform: (chunk, _, cb) => {
			if (!settled) {
				totalLength += chunk.byteLength;
				if (expected == null || totalLength <= expected)
					buffers.push(chunk);
				else
					settled = true, buffers = [];
			}
			cb(null, chunk);
		},
		final: (cb) => {
			if (!settled && (expected == null || totalLength == expected))
				callback(Buffer.concat(buffers));
			settled = true;
			cb(null);
		}
	});

	/* pipe the source stream data through the sniffer and ensure errors cleanup the internal state properly */
	libStream.pipeline(stream, sniffer, (err: any) => {
		if (err != null)
			settled = true, buffers = [];
	});
	return sniffer;
}
async function atomicWrite(path: string, content: Buffer | libStream.Readable, logger: libLog.Logger, options?: { what?: string, temporary?: string, create?: boolean, mtime?: number }): Promise<void> {
	/* default temporary names are unique to prevent concurrent writes to the same path from corrupting each other */
	const tempPath = (options?.temporary ?? `${path}.${process.pid}-${++nextTemporaryId}.temp`), create = (options?.create === true);
	if (options?.what != '')
		logger.trace(`Writing ${options?.what ?? 'data'} to [${path}]`);

	/* eagerly create the destination file to let creation failures propagate out immediately and separately from any write failures (for
	*	create: to detect an already existing file; an open-failure leaves the content stream unconsumed, hence release it explicitly) */
	let handle: libFsPromises.FileHandle;
	try {
		handle = await libFsPromises.open((create ? path : tempPath), (create ? 'wx' : 'w'));
	} catch (err: any) {
		if (content instanceof libStream.Readable)
			content.destroy();
		if (create)
			throw wrapError(err, 'Create file');
		throw wrapError(err, `Create temporary file [${tempPath}]`);
	}

	let written = false;
	try {
		/* check if a stream or buffer is being written (the file stream takes
		*	ownership of the handle and closes it upon completion or destruction) */
		if (content instanceof libStream.Readable) {
			await new Promise<void>((resolve, reject) => {
				libStream.pipeline(content, handle.createWriteStream(), (err: any) => {
					if (err == null)
						resolve();
					else
						reject(err);
				});
			});
		}
		else {
			try { await handle.writeFile(content); }
			finally { await handle.close().catch(() => { }); }
		}

		/* apply the explicit modified-time (before the rename, to let the file be moved into place fully
		*	configured; as fractional seconds, to prevent sub-millisecond stat values from being truncated) */
		if (options?.mtime != null)
			await libFsPromises.utimes((create ? path : tempPath), options.mtime / 1000, options.mtime / 1000);
		written = true;

		/* move the file into place */
		if (!create)
			await libFsPromises.rename(tempPath, path);
	} catch (err: any) {
		if (options?.what != '')
			logger.trace(`Removing partially written [${create ? path : tempPath}]`);

		/* try to remove the partial/temporary file */
		try { await libFsPromises.unlink(create ? path : tempPath); }
		catch (err: any) {
			if (err.code != 'ENOENT')
				logger.warning(`Failed to remove temporary file [${create ? path : tempPath}]: ${err.message}`);
		}

		if (written)
			throw wrapError(err, 'Replace the original file');
		else if (create)
			throw wrapError(err, 'Write to file');
		throw wrapError(err, `Write to temporary file [${tempPath}]`);
	}
}

interface CacheEntry {
	data: Buffer;
	encodings: Record<string, Buffer>;
	mtime: number;
	age: number;
	touched: number;
}
class CacheManager {
	private logger: libLog.Logger;
	private map: Record<string, CacheEntry> = {};
	private nextStamp: number = 0;
	private allocated: number = 0;
	private totalCapacity: number;
	private largestSize: number;
	private nextAge: number;

	public constructor(logger: libLog.Logger, cacheSize: number, fileSizeLimit: number) {
		this.logger = logger;
		this.totalCapacity = cacheSize;
		this.largestSize = fileSizeLimit;
		this.nextAge = 0;
	}

	private reduce(maximum: number): void {
		/* create the list of all cached objects sorted by the touched count */
		const paths: string[] = Object.keys(this.map).sort((a, b) => this.map[a].touched - this.map[b].touched);

		/* drop the first half of the entries or more, if the size has not yet been freed */
		for (let i = 0; i < (paths.length + 1) / 2 || this.allocated > maximum; ++i)
			this.drop(paths[i]);
	}

	public add(path: string, data: Buffer, mtime: number, age: number): void {
		if (!this.cacheable(data.byteLength))
			return;

		/* check if the entry is already part of the cache and check if the current entry should be evicted */
		if (path in this.map) {
			const entry = this.map[path];
			if (entry.age >= age)
				return;
			this.drop(path);
		}

		/* check if space needs to be reserved */
		if (this.allocated + data.byteLength > this.totalCapacity)
			this.reduce(this.totalCapacity - data.byteLength);

		/* add the entry to the cache */
		this.allocated += data.byteLength;
		this.map[path] = { data, encodings: {}, mtime, touched: ++this.nextStamp, age };

		this.logger.log(`Added [${path}] to the cache (Size: ${data.byteLength} / Allocated: ${this.allocated})`);
	}
	public addEncoding(path: string, data: Buffer, age: number, name: string): void {
		if (!this.cacheable(data.byteLength))
			return;

		/* check if the entry is still in the cache */
		if (!(path in this.map) || this.map[path].age != age)
			return;

		/* check if the encoding already exists */
		if (name in this.map[path].encodings)
			return;

		/* check if space needs to be reserved and check if the root entry is still available afterwards */
		if (this.allocated + data.byteLength > this.totalCapacity)
			this.reduce(this.totalCapacity - data.byteLength);
		if (!(path in this.map))
			return;

		/* add the entry to the cache */
		this.allocated += data.byteLength;
		this.map[path].encodings[name] = data;

		this.logger.log(`Added encoding [${name}] of [${path}] to the cache (Size: ${data.byteLength} / Allocated: ${this.allocated})`);
	}
	public drop(path: string): void {
		if (!(path in this.map))
			return;

		/* remove all cached encodings and the entry itself */
		const entry = this.map[path];
		for (const key in entry.encodings)
			this.allocated -= entry.encodings[key].byteLength;
		this.allocated -= entry.data.byteLength;
		delete this.map[path];

		this.logger.log(`Dropped [${path}] and encodings from the cache (Allocated: ${this.allocated})`);
	}
	public cacheable(size: number): boolean {
		return (size <= this.largestSize && size <= this.totalCapacity);
	}
	public flush(): void {
		this.map = {};
		this.allocated = 0;
	}
	public find(path: string, stats: { mtime: number, size: number } | null): CacheEntry | null {
		const entry = this.map[path] ?? null;
		if (entry == null)
			return null;

		/* check if the entry is still up-to-date and return it */
		if (stats == null || (entry.mtime == stats.mtime && entry.data.length == stats.size)) {
			entry.touched = ++this.nextStamp;
			return entry;
		}

		/* remove the entry as it seems to be outdated */
		this.drop(path);
		return null;
	}
	public allocAge(): number {
		return ++this.nextAge;
	}
}

interface ImmutableEntry {
	identifier: string;
	immutable: string;
	unique: string;
	path: string;
	fileSystem: string | null;
	size: number;
	mtime: number;
	fetched: boolean;
}
interface ImmutableSerialized {
	identifier: string;
	unique: string;
	path: string;
	fileSystem: string | null;
	size: number;
	mtime: number;
}
class ImmutableManager {
	private logger: libLog.Logger;
	private map: Record<string, ImmutableEntry> = {};
	private reverse: Record<string, string> = {};
	private writeBack: { path: string, writing: Promise<void> | null, dirty: boolean } | null = null;
	private alwaysValidate: boolean;
	private immutableTagging: boolean;

	constructor(writeBackPath: string, logger: libLog.Logger, alwaysValidate: boolean, immutableTagging: boolean) {
		this.logger = logger;
		this.alwaysValidate = alwaysValidate;
		this.immutableTagging = immutableTagging;

		if (writeBackPath != '')
			this.configureWriteBack(writeBackPath);
	}

	private assignId(entry: ImmutableEntry): void {
		const firstId = (entry.unique == '');
		if (!firstId)
			delete this.reverse[entry.unique];

		/* setup the new unique id */
		entry.unique = '';
		for (let i = 0; i < UNIQUE_ID_LENGTH; ++i)
			entry.unique += UNIQUE_ID_CHARS[libCrypto.randomInt(UNIQUE_ID_CHARS.length)];

		/* patch the state up to contain the new id */
		const [base, name, extension] = libHelper.splitFileExtension(entry.path);
		entry.immutable = `${base}${name}.${entry.unique}${extension}`;
		this.reverse[entry.unique] = entry.identifier;

		this.logger.trace(`${firstId ? 'Allocated' : 'Re-allocated'} immutable unique id [${entry.unique}] for [${entry.identifier}]`);
	}
	private async storeState(): Promise<void> {
		if (this.writeBack == null)
			return;
		this.writeBack.dirty = true;
		if (this.writeBack.writing != null)
			return;

		let resolver = () => { };
		this.writeBack.writing = new Promise((res) => resolver = res);

		/* check if the state is dirty and perform the write backs */
		while (this.writeBack.dirty) {
			this.writeBack.dirty = false;

			/* collect the list of all relevant entries */
			let output: ImmutableSerialized[] = [];
			for (const identifier in this.map) {
				const entry = this.map[identifier];
				output.push({
					unique: entry.unique,
					identifier,
					path: entry.path,
					fileSystem: entry.fileSystem,
					size: entry.size,
					mtime: entry.mtime
				});
			}
			const content: string = JSON.stringify(output);

			/* ignore any read/write failures */
			try {
				await atomicWrite(this.writeBack.path, Buffer.from(content, 'utf-8'), this.logger, { what: 'immutable state' });
			} catch (err: any) {
				this.logger.error(`Failed to write immutable state to [${this.writeBack.path}]: ${err.message}`);
			}
		}

		this.writeBack.writing = null;
		resolver();
	}
	private async loadState(path: string): Promise<ImmutableSerialized[]> {
		this.logger.trace(`Loading immutable state from [${path}]`);

		/* load the current file and parse it as json (skip any content failed to be read or if the file did not exist) */
		let state: unknown = null;
		try {
			state = JSON.parse(await libFsPromises.readFile(path, { encoding: 'utf-8' }));
		} catch (err: any) {
			if (err.code != 'ENOENT')
				this.logger.error(`Error while loading immutable state from [${path}]: ${err.message}`);
			return [];
		}

		if (!Array.isArray(state)) {
			this.logger.error(`Immutable state [${path}] is malformed (discarding state)`);
			return [];
		}

		/* parse all of the values and validate their general structure */
		let corrupted = 0, output: ImmutableSerialized[] = [];
		for (const entry of state) {
			if (typeof entry.unique != 'string' || typeof entry.identifier != 'string' || (typeof entry.fileSystem != 'string' && entry.fileSystem !== null))
				++corrupted;
			else if (!`.${entry.unique}`.match(ID_EXTENSION_REGEX) || typeof entry.path != 'string')
				++corrupted;
			else if (typeof entry.mtime != 'number' || !isFinite(entry.mtime) || entry.mtime < 0)
				++corrupted;
			else if (typeof entry.size != 'number' || !isFinite(entry.size) || entry.size < 0 || Math.floor(entry.size) != entry.size)
				++corrupted;
			else
				output.push({ unique: entry.unique, identifier: entry.identifier, path: entry.path, fileSystem: entry.fileSystem, size: entry.size, mtime: entry.mtime });
		}

		if (corrupted > 0)
			this.logger.error(`Immutable loaded state [${path}] contained [${corrupted}] malformed entries`);
		return output;
	}
	private updateEntry(entry: ImmutableEntry, checkFreshness: boolean, firstAssign: boolean): boolean {
		if (entry.fetched && !checkFreshness && !this.alwaysValidate)
			return true;

		/* fetch the file states of the actual filesystem entry */
		const stats = readStats(entry.fileSystem!);
		if (stats == null) {
			this.logger.warning(`Immutable path [${entry.fileSystem}] does not exist`);
			delete this.map[entry.identifier];
			delete this.reverse[entry.unique];
			this.storeState();
			return false;
		}
		const [fileSize, mtime] = stats;
		entry.fetched = true;

		/* check if the stats have changed, in which case a new unique-id needs to be assigned (thus binding the it to the stats) */
		if (!firstAssign) {
			if (entry.size == fileSize && entry.mtime == mtime)
				return true;
			this.assignId(entry);
		}
		entry.size = fileSize;
		entry.mtime = mtime;
		this.storeState();
		return true;
	}
	private async configureWriteBack(path: string): Promise<void> {
		let states = await this.loadState(path);

		/* merge the loaded state into the current state (prefer current state over
		*	loaded state; current state might already exist after awaiting the load) */
		const performInitialWriteBack = (Object.keys(this.map).length > 0);
		for (const state of states) {
			if (state.identifier in this.map)
				continue;
			this.logger.trace(`Recovering immutable unique id [${state.unique}] for [${state.identifier}]`);

			const [base, name, extension] = libHelper.splitFileExtension(state.path);
			this.map[state.identifier] = {
				immutable: `${base}${name}.${state.unique}${extension}`,
				identifier: state.identifier,
				path: state.path,
				fileSystem: state.fileSystem,
				size: state.size,
				mtime: state.mtime,
				unique: state.unique,
				fetched: false
			};
			this.reverse[state.unique] = state.identifier;
		}

		/* configure the actual writeback and check if an initial store needs to be triggered */
		this.writeBack = { path, writing: null, dirty: false };
		if (performInitialWriteBack)
			this.storeState();
	}

	public make(module: string, path: string, checkFreshness: boolean): string {
		const identifier = `${module}:${path}`;
		if (!this.immutableTagging)
			return path;

		while (true) {
			/* check if the entry does not yet exist and the id-tagged path needs to be created */
			let entry = this.map[identifier] ?? null;
			if (entry == null) {
				entry = (this.map[identifier] = { immutable: '', identifier, path, unique: '', fileSystem: null, size: 0, mtime: 0, fetched: false });
				this.assignId(entry);
				this.storeState();
			}

			/* check if the stats can actually be validated or if the entry should just be served (if
			*	the file does not exist/has been removed, simply restart the loop with to get a fresh id) */
			else if (entry.fileSystem != null) {
				try {
					if (!this.updateEntry(entry, checkFreshness, false))
						continue;
				} catch (err: any) {
					this.logger.warning(`Failed to validate immutable entry [${identifier}] and assuming unmodified: ${err.message}`);
				}
			}
			return entry.immutable;
		}
	}
	public get(path: string, checkFreshness: boolean): [ImmutableEntry | null, boolean] {
		if (!this.immutableTagging)
			return [null, false];

		/* check if it might be an immutable-tagged path (for files without an extension, the id itself is the last extension) */
		let [base, temp, extension] = libHelper.splitFileExtension(path);
		let [_, name, tempId] = libHelper.splitFileExtension(temp);
		if (tempId == '')
			tempId = extension, extension = '', name = temp;
		if (!tempId.match(ID_EXTENSION_REGEX))
			return [null, false];
		const unique = tempId.substring(1);

		/* check if the entry does not exist in the reverse mapping anymore - indicating that the unique-id
		*	is old/outdated, in which case it can be recovered by comparing the full actual path to all of
		*	the entries, looking for the actual object, thereby recovering the most recent immutable path */
		let identifier = this.reverse[unique] ?? null;
		if (identifier == null) {
			const fileSystemPath = `${base}${name}${extension}`;
			for (const tempIdentifier in this.map) {
				if (this.map[tempIdentifier].fileSystem != fileSystemPath)
					continue;
				identifier = tempIdentifier;
				break;
			}
			if (identifier == null)
				return [null, false];
		}
		const entry = this.map[identifier]!, firstAssign = (entry.fileSystem == null);
		if (entry.fileSystem == null)
			entry.fileSystem = `${base}${name}${extension}`;

		/* update the entry and return the final stats/if the unique-id changed */
		if (!this.updateEntry(entry, checkFreshness, firstAssign))
			return [null, false];
		return [entry, entry.unique != unique];
	}
	public invalidate(): void {
		for (const identifier in this.map)
			this.map[identifier].fetched = false;
	}
}

class AlreadyCached implements Cached {
	private cache: CacheManager;
	private path: string;
	private entry: CacheEntry;
	private immutable: boolean;

	public constructor(cache: CacheManager, path: string, entry: CacheEntry, immutable: boolean) {
		this.cache = cache;
		this.path = path;
		this.entry = entry;
		this.immutable = immutable;
	}

	public isImmutable(): boolean {
		return this.immutable;
	}
	public filePath(): string {
		return this.path;
	}
	public fileSize(): number {
		return this.entry.data.byteLength;
	}
	public lastModified(): string {
		return new Date(this.entry.mtime).toUTCString();
	}
	public timeModified(): number {
		return this.entry.mtime;
	}
	public uniqueId(): string {
		return `${this.entry.mtime}-${this.entry.data.byteLength}`;
	}
	public stream(options?: { start?: number, end?: number, eager?: boolean }): libStream.Readable {
		return libStream.Readable.from(this.entry.data.subarray(options?.start, (options?.end == null ? undefined : options.end + 1)));
	}
	public async read(): Promise<Buffer> {
		return this.entry.data;
	}
	public readSync(): Buffer {
		return this.entry.data;
	}
	public encoded(encoding?: libBase.EncodingType): EncodedCache {
		return EncodedCache(this.cache, this, this.entry, this.entry.age, encoding);
	}
}
class NotCached implements Cached {
	private cache: CacheManager;
	private path: string;
	private size: number;
	private mtime: number;
	private immutable: boolean;
	private age: number;

	public constructor(cache: CacheManager, path: string, size: number, mtime: number, age: number, immutable: boolean) {
		this.cache = cache;
		this.path = path;
		this.size = size;
		this.mtime = mtime;
		this.immutable = immutable;
		this.age = age;
	}
	private makeStream(options?: { start?: number, end?: number, eager?: boolean }): libStream.Readable {
		let stream: libFs.ReadStream | null = null;

		/* create the data stream (eager: open the file immediately and let creation failures
		*	throw; otherwise open failures will be emitted as errors on the stream itself) */
		if (options?.eager === true) {
			const fd = libFs.openSync(this.path, 'r');
			stream = libFs.createReadStream('', { fd, start: options?.start, end: options?.end });
		}
		else try {
			stream = libFs.createReadStream(this.path, { flags: 'r', start: options?.start, end: options?.end });
		} catch (err: any) {
			return new libStream.Readable({ read() { this.destroy(wrapError(err, 'Reading file')) } });
		}

		/* check if only a partial file is being read, or it is too large, in which case it will not be added to the cache */
		if (!this.cache.cacheable(this.size) || (options?.start != null && options.start != 0) || (options?.end != null && options.end + 1 != this.size))
			return stream;

		/* create the stream-sniffer to collect the data and write them to the cache */
		return sniffStream(stream, this.size, (data: Buffer) => this.cache.add(this.path, data, this.mtime, this.age));
	}

	public isImmutable(): boolean {
		return this.immutable;
	}
	public filePath(): string {
		return this.path;
	}
	public fileSize(): number {
		return this.size;
	}
	public lastModified(): string {
		return new Date(this.mtime).toUTCString();
	}
	public timeModified(): number {
		return this.mtime;
	}
	public uniqueId(): string {
		return `${this.mtime}-${this.size}`;
	}
	public stream(options?: { start?: number, end?: number, eager?: boolean }): libStream.Readable {
		return this.makeStream(options);
	}
	public async read(): Promise<Buffer> {
		return StreamToAsync(this.makeStream());
	}
	public readSync(): Buffer {
		/* just let the errors propagate out */
		const data = libFs.readFileSync(this.path);

		/* add the read buffer back to the cache (using the fetched data from before reading the file - to
		*	detect a file-change since before reading the file; dont error as stream also just proceeds) */
		if (data.byteLength == this.size)
			this.cache.add(this.path, data, this.mtime, this.age);
		return data;
	}
	public encoded(encoding?: libBase.EncodingType): EncodedCache {
		return EncodedCache(this.cache, this, null, this.age, encoding);
	}
}

function StreamToAsync(stream: libStream.Readable): Promise<Buffer> {
	return new Promise((resolve, reject) => {
		const buffers: Buffer[] = [];
		let settled: boolean = false;

		/* register the handler to collect the data and stream them out */
		stream.on("data", (chunk) => {
			if (!settled)
				buffers.push(chunk);
		});
		stream.once("error", (err) => {
			if (!settled) {
				settled = true;
				reject(err);
			}
		});
		stream.once("end", () => {
			if (!settled) {
				settled = true;
				resolve(Buffer.concat(buffers));
			}
		});
	});
}
function EncodedCache(cache: CacheManager, reader: Cached, entry: CacheEntry | null, age: number, encoding?: libBase.EncodingType): EncodedCache {
	/* check if no encoding is used, in which case the source reader can just be wrapped */
	if (encoding == null) {
		return {
			contentSize: () => reader.fileSize(),
			stream: (options?: { eager?: boolean }) => reader.stream({ eager: options?.eager }),
			read: () => reader.read(),
			readSync: () => reader.readSync()
		};
	}

	/* check if the given encoding has already been cached */
	if (entry != null && encoding.name in entry.encodings) {
		const encoded = entry.encodings[encoding.name];
		return {
			contentSize: () => encoded.byteLength,
			stream: () => libStream.Readable.from(encoded),
			read: async () => encoded,
			readSync: () => encoded
		};
	}

	const makeStream: (eager?: boolean) => libStream.Readable = (eager?: boolean) => {
		/* create the encoded pipe (eager: let creation failures of the source stream throw) */
		const stream = reader.stream({ eager });
		const encoder = libStream.pipeline(stream, encoding.makeEncode(), () => { });

		/* check if there is even a chance for the data to be cached and then attach the
		*	sniffer to collect the encoded data (encoded size is not known upfront) */
		if (!cache.cacheable(reader.fileSize()))
			return encoder;
		return sniffStream(encoder, null, (data: Buffer) => cache.addEncoding(reader.filePath(), data, age, encoding.name));
	};

	/* wrap the original reader around the encoder (let errors propagate out) */
	return {
		contentSize: () => null,
		stream: (options?: { eager?: boolean }) => makeStream(options?.eager),
		read: () => StreamToAsync(makeStream()),
		readSync: () => {
			/* will be returned as buffer anyways, so might as well try to write it
			*	back to the cache (automatically rejected again, if its too large) */
			const data = encoding.encodeBuffer(reader.readSync());
			cache.addEncoding(reader.filePath(), data, age, encoding.name);
			return data;
		}
	};
}

/** cached file entry to interact with (all errors follow the CacheHost error code conventions) */
export interface Cached {
	/** check if this is an immutable file entry */
	isImmutable(): boolean;

	/** path of the file */
	filePath(): string;

	/** size in bytes of the file */
	fileSize(): number;

	/** fetch the last modified time formatted for the network */
	lastModified(): string;

	/** fetch the last modified time in milliseconds since the epoch */
	timeModified(): number;

	/** fetch the unique-id to identify this version of the cached file (constructed,
	 *	just like the cache identifies them as equivalent: size+last-modified) */
	uniqueId(): string;

	/** [throws if eager, otherwise no-throw but errors, register 'error' handler] object or encoded entry must not be used anymore
	 *	after reading or streaming from it; if [eager], the underlying file is opened at creation and creation failures throw (the
	 *	returned stream must then be consumed or destroyed to release the file); otherwise creation failures error on the stream */
	stream(options?: { start?: number, end?: number, eager?: boolean }): libStream.Readable;

	/** [throws] object or encoded entry must not be used anymore after reading or streaming from it */
	read(): Promise<Buffer>;

	/** [throws] object or encoded entry must not be used anymore after reading or streaming from it */
	readSync(): Buffer;

	/** create an encoded version of this cached entry (no encoding is equivalent to identity) */
	encoded(encoding?: libBase.EncodingType): EncodedCache;
}

/** encoded view of a cached file entry (all errors follow the CacheHost error code conventions) */
export interface EncodedCache {
	/** size in bytes of the encoding (null if not yet determined) */
	contentSize(): number | null;

	/** [throws if eager, otherwise no-throw but errors, register 'error' handler] object or encoded entry must not be used anymore
	 *	after reading or streaming from it; if [eager], the underlying file is opened at creation and creation failures throw (the
	 *	returned stream must then be consumed or destroyed to release the file); otherwise creation failures error on the stream */
	stream(options?: { eager?: boolean }): libStream.Readable;

	/** [throws] object or encoded entry must not be used anymore after reading or streaming from it */
	read(): Promise<Buffer>;

	/** [throws] object or encoded entry must not be used anymore after reading or streaming from it */
	readSync(): Buffer;
}

/** wrapper for readable stream, which also exposes the file size and optionally the last modified time in milliseconds */
export interface FileReadable extends libStream.Readable {
	fileSize: number;
	timeModified?: number;
}

/** create a file-readable stream from a normal stream (stream should exactly produce the given number of bytes, or an error; other behavior
 *	may be treated as an error; only streams branded by this function are recognized as sized - such as by isFileReadable or write) */
export function createFileReadable(stream: libStream.Readable, fileSize: number, timeModified?: number): FileReadable {
	const wrapped = stream as FileReadable;
	wrapped.fileSize = fileSize;
	wrapped.timeModified = timeModified;
	(wrapped as any)[FileReadableBrand] = true;
	return wrapped;
}

/** check if the object is a sized-readable stream created via createFileReadable */
export function isFileReadable(stream: unknown): stream is FileReadable {
	return (stream instanceof libStream.Readable && (stream as any)[FileReadableBrand] === true);
}

/**
 *	all cache host operations which may throw errors and will not log them, and dont guarantee to contain the failing file path; every
 *	error - thrown or emitted by a returned stream - carries the [code] property of the underlying failure (system error codes such as
 *	ENOENT, EACCES, ENOSPC, ..., or node/zlib stream codes); errors originating from caller-provided streams (such as content streams
 *	passed to write) only carry whatever [code] the caller attached to them, which may be none; errors rewrapped for context preserve
 *	the code and expose the original via [cause]; file operations with the cache ignore symlinks and will follow them at all times.
 */
export class CacheHost extends libLog.Logger {
	private _cacheManager: CacheManager;
	private _immutableManager: ImmutableManager;
	private _config: BurntCacheConfig;

	public constructor(config?: CacheConfig | BurntCacheConfig) {
		super('cache');

		this.info('Cache created');
		this._config = BurntCacheConfig.from(config);
		libHelper.logConfiguration(this._config, this);

		this._cacheManager = new CacheManager(this, this._config.cacheSize, this._config.fileSizeLimit);
		this._immutableManager = new ImmutableManager(this._config.immutableStatePath, this, this._config.alwaysValidate, this._config.immutableTagging);
	}
	private resolveCache(path: string, checkFreshness: boolean, checkImmutable: boolean): Cached | string | null {
		/* check if the entry is immutable and fetch its actual path or
		*	if it has been moved (due to the id having been invalidated) */
		const [immutable, immutableMoved] = (checkImmutable ? this._immutableManager.get(path, checkFreshness) : [null, false]);
		if (immutable != null) {
			path = immutable.fileSystem!;
			if (immutableMoved)
				return immutable.immutable;
		}
		const isImmutable = (immutable != null);

		/* check if the entry is already in the cache and no freshness validation is required, in which case the file itself does not need to be checked */
		let entry = ((checkFreshness || this._config.alwaysValidate) ? null : this._cacheManager.find(path, null));
		if (entry != null && (immutable == null || (immutable.size == entry.data.byteLength && immutable.mtime == entry.mtime)))
			return new AlreadyCached(this._cacheManager, path, entry, isImmutable);

		/* check if the file exists and read its file-size and mtime (not necessary
		*	for immutable entries, as their stats are already up-to-date) */
		const stats = (immutable == null ? readStats(path) : [immutable.size, immutable.mtime]);
		if (stats == null)
			return null;
		const [fileSize, mtime] = stats;

		/* check if the path exists in the validated cache and otherwise return the uncached entry */
		entry = this._cacheManager.find(path, { mtime, size: fileSize });
		if (entry != null)
			return new AlreadyCached(this._cacheManager, path, entry, isImmutable);
		return new NotCached(this._cacheManager, path, fileSize, mtime, this._cacheManager.allocAge(), isImmutable);
	}

	/** configuration used by this cache host */
	public get config(): BurntCacheConfig {
		return this._config;
	}

	/** [throws] if [checkFreshness] is true, re-validate the file stats on disk before serving from cache (defaults to
	 *	false); resolve immutable ids automatically (Cached to interact with cache; null, if it does not exist, string if
	 *	the immutable path has been permanently moved to the new path in source space; the redirected path will be in the
	 *	same shape as originally used to create the immutable path: relative to the owner module and URI encoded) */
	public fetchImmutable(path: string, options?: { checkFreshness?: boolean }): Cached | string | null {
		return this.resolveCache(path, options?.checkFreshness ?? false, true);
	}

	/** [throws] if [checkFreshness] is true re-validate the file stats on disk before serving from cache (defaults
	 *	to false); no immutable ids are resolved (Cached to interact with cache; null, if it does not exist) */
	public fetchDirect(path: string, options?: { checkFreshness?: boolean }): Cached | null {
		return this.resolveCache(path, options?.checkFreshness ?? false, false) as (Cached | null);
	}

	/** generate a unique tagged path for the given query path, which will change whenever the underlying file changes;
	 *	[checkFreshness]: if true, re-validate the file stats on disk to detect changes (defaults to false); creates a path
	 *	to a file, which looks similar to the source, except that the name includes a unique id, which will be used to
	 *	identify the given file state (will be removed from the final target path to be served, to identify the actual source);
	 *	[path] must be given in the module's path space (i.e. before applying client.makePath), and URI encoded as this path
	 *	will be used as basis for redirects in case of outdated id's; path's URI encoding is preserved by the produced path */
	public immutable(module: string, path: string, options?: { checkFreshness?: boolean }): string {
		return this._immutableManager.make(module, path, options?.checkFreshness ?? false);
	}

	/** flush all cached data and invalidate immutable stats so they are re-checked on next access */
	public flush(): void {
		this.info('Flushing cache and invalidating immutable entries');
		this._cacheManager.flush();
		this._immutableManager.invalidate();
	}

	/** [throws] read the data directly into a buffer (designed for modules to interact with) */
	public async read(path: string, options?: { checkFreshness?: boolean }): Promise<Buffer | null> {
		try {
			const entry = this.resolveCache(path, options?.checkFreshness ?? false, false) as (Cached | null);
			if (entry == null)
				return null;

			/* await in-place to ensure errors are caught and pre-processed */
			return await entry.read();
		}

		/* special abstraction to ensure check-before-use does not result in not-found */
		catch (err: any) {
			if (err.code == 'ENOENT')
				return null;
			throw err;
		}
	}

	/** [throws if eager, otherwise no-throw but errors, register 'error' handler] create a read stream of the data
	 *	(designed for modules to interact with); if [eager], the file is opened at creation: creation failures throw,
	 *	a file having vanished since the lookup returns null, and the returned stream must be consumed or destroyed to
	 *	release the file; otherwise creation failures error on the stream (in which case ENOENT cannot be mapped to null) */
	public stream(path: string, options?: { checkFreshness?: boolean, eager?: boolean }): FileReadable | null {
		try {
			const entry = this.resolveCache(path, options?.checkFreshness ?? false, false) as (Cached | null);
			if (entry == null)
				return null;
			return createFileReadable(entry.stream({ eager: options?.eager }), entry.fileSize(), entry.timeModified());
		}

		/* special abstraction to ensure check-before-use does not result in not-found */
		catch (err: any) {
			if (err.code == 'ENOENT')
				return null;
			throw err;
		}
	}

	/** [throws] write data atomically to the disk and conditionally update the cache (designed for modules
	 *	to interact with; writes as utf-8; writes data first to temporary file and then replaces the file
	 *	atomically; for create: must not replace an existing file and writes directly to the destination,
	 *	hence not atomically; for mtime: apply the given modified-time in milliseconds since the epoch to
	 *	the written file; empty what string will not log anything) */
	public async write(path: string, data: Buffer | string | libStream.Readable | FileReadable, options?: { what?: string, temporary?: string, create?: boolean, mtime?: number }): Promise<void> {
		let collected: Buffer | null = null;
		if (typeof data == 'string')
			data = Buffer.from(data, 'utf-8');

		/* check if the data are an instance of the sized stream and collect the data to be written to the cache */
		else if (isFileReadable(data) && this._cacheManager.cacheable(data.fileSize))
			data = sniffStream(data, data.fileSize, (sniffed: Buffer) => { collected = sniffed; });

		/* write the data atomically to the destination (let errors propagate out) */
		await atomicWrite(path, data, this, { what: (options?.what ?? 'via cache'), temporary: options?.temporary, create: options?.create, mtime: options?.mtime });

		/* check if the data are available and can be written to the cache (unsized streamed data will not be cached, as their
		*	size cannot be determined; drop any existing cache entry in that case, as it now contains outdated content) */
		if (collected != null)
			data = collected;
		if (data instanceof libStream.Readable || !this._cacheManager.cacheable(data.byteLength)) {
			this._cacheManager.drop(path);
			return;
		}
		const age = this._cacheManager.allocAge();

		/* fetch the new state and update the cache (let errors propagate out; only if the write-state seems consistent) */
		const stats = readStats(path);
		if (stats == null)
			this._cacheManager.drop(path);
		else if (stats[0] == data.byteLength)
			this._cacheManager.add(path, data, stats[1], age);
	}

	/** [throws] remove the data from the physical disk and from the cache (designed
	 *	for modules to interact with; returns false if it did not exist) */
	public async remove(path: string): Promise<boolean> {
		let existed: boolean = true;

		/* try to remove the physical file */
		try { await libFsPromises.unlink(path); }
		catch (err: any) {
			if (err.code != 'ENOENT')
				throw err;
			existed = false;
		}

		/* remove the data from the cache */
		this._cacheManager.drop(path);
		return existed;
	}
}

/** simple wrapper function to create a cache */
export function createCache(config?: CacheConfig | BurntCacheConfig): CacheHost {
	return new CacheHost(config);
}

export interface CacheConfig {
	/** immutable state path is used to ensure the immutable state uses persistent ids across
	 *	restarts (will be read upon loading; if not set, ids will be lost after a server restart) [Default: ''] */
	immutableStatePath?: string;

	/** total cachable size [Default: 50_000_000] */
	cacheSize?: number;

	/** upper limit for files considered cachable, others will just be streamed through [Default: 10_000_000] */
	fileSizeLimit?: number;

	/** always validate file freshness before providing them [Default: false] */
	alwaysValidate?: boolean;

	/** tag served content with immutable ids to encode freshness into the path [Default: true] */
	immutableTagging?: boolean;
}

export class BurntCacheConfig {
	public readonly immutableStatePath: string;
	public readonly cacheSize: number;
	public readonly fileSizeLimit: number;
	public readonly alwaysValidate: boolean;
	public readonly immutableTagging: boolean;

	public constructor(config?: CacheConfig) {
		this.immutableStatePath = config?.immutableStatePath ?? '';
		this.cacheSize = config?.cacheSize ?? 50_000_000;
		this.fileSizeLimit = config?.fileSizeLimit ?? 10_000_000;
		this.alwaysValidate = config?.alwaysValidate ?? false;
		this.immutableTagging = config?.immutableTagging ?? true;
	}

	public static from(config?: CacheConfig | BurntCacheConfig): BurntCacheConfig {
		return (config instanceof BurntCacheConfig ? config : new BurntCacheConfig(config));
	}
}
