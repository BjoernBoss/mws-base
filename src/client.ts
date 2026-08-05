/* SPDX-License-Identifier: BSD-3-Clause */
/* Copyright (c) 2024-2026 Bjoern Boss Henrichsen */
import * as libLog from "./log.js";
import * as libBuilder from "./builder.js";
import * as libCache from "./cache.js";
import * as libHelper from "./helper.js";
import * as libBase from "./base.js";
import * as libServer from "./server.js";
import * as libEvents from "events";
import * as libStream from "stream";
import * as libUrl from "url";
import * as libWs from "ws";
import * as libHttp from "http";

const BAD_HTTP_STRING_REGEX: RegExp = /[\x00-\x1f\x7f]|[^\x00-\xff]/;
const BAD_HTTP_HEADER_NAME_REGEX: RegExp = /[\x00-\x1f\x7f\(\)<>@,;:\\"/\[\]\?=\{\} \t]|[^\x00-\x7f]/;

class ClientContext {
	public path: string;
	public identity: string;
	public translationCount: number;
	public busyCount: number;
	public patchCount: number;

	constructor(path: string, identity: string, translationCount: number, busyCount: number, patchCount: number) {
		this.identity = identity;
		this.path = path;
		this.translationCount = translationCount;
		this.busyCount = busyCount;
		this.patchCount = patchCount;
	}
}

class ClientBase extends libLog.Logger {
	private _server: libServer.Server;
	private _config: BurntClientConfig;
	protected _path: string;
	protected _url: libUrl.URL;
	protected _translation: Record<string, string | null>[];

	protected constructor(url: libUrl.URL, kind: string, server: libServer.Server, config: BurntClientConfig);
	protected constructor(client: ClientBase, kind: string, server: libServer.Server, config: BurntClientConfig);
	protected constructor(arg: libUrl.URL | ClientBase, kind: string, server: libServer.Server, config: BurntClientConfig) {
		super(kind);

		if (arg instanceof libUrl.URL) {
			this._translation = [];
			this._path = arg.pathname;
			this._url = arg;
		}
		else {
			this._translation = arg._translation;
			this._path = arg._path;
			this._url = arg._url;
		}

		this._server = server;
		this._config = config;
	}

	private makeTranslatedPath(path: string): string {
		let original = path, unmapped = false;

		for (let i = this._translation.length - 1; i >= 0; --i) {
			let match: [string, string] | null = null;

			/* find the best reverse mapping and apply it */
			for (const [from, to] of Object.entries(this._translation[i])) {
				if (to != null && libHelper.isSubPath(to, path) && (match == null || match[1].length < to.length))
					match = [from, to];
			}

			/* check if a mapping has been found and reverse-apply it and otherwise use an implicit identity mapping (no need to
			*	check any forward mappings as no certain decisions can be made until the path has been fully reverse-translated) */
			if (match != null)
				path = libHelper.rebasePath(match[1], match[0], path);
			else
				unmapped = true;
		}

		/* check if the final path was partially unmapped */
		if (unmapped)
			this.warning(`Path [${original}] is not mapped by translations`);
		return path;
	}

	/** raw request origin (host will be lower-case; NOT sanitized) */
	public get url(): libUrl.URL {
		return this._url;
	}

	/** path relative to current module (fully sanitized and clean path, does not end in a slash, unless its root; remains
	 *	URI encoded in the canonical form; only characters, which may not appear literally in a path component, remain encoded) */
	public get path(): string {
		return this._path;
	}

	/** server the client originates from */
	public get server(): libServer.Server {
		return this._server;
	}

	/** cache host used by this server and client */
	public get cache(): libCache.CacheHost {
		return this._server.cache;
	}

	/** configuration used by this client */
	public get config(): BurntClientConfig {
		return this._config
	}

	/** create a path relative from the current module into the clients traversed server space (path must be properly URI encoded and will be normalized
	 *	and sanitized to the canonical encoding; returns a canonically URI encoded path, as translations guarantee to also be properly URI encoded) */
	public makePath(path: string): string {
		/* normalize and sanitize the path */
		const [sanitized, valid] = libHelper.normalizeEncodedPath(path);
		if (!valid)
			this.warning(`Path [${path}] contains malformed URI encoding`);
		return this.makeTranslatedPath(sanitized);
	}

	/** create a path tagged with an immutable id relative from the current module into the clients traversed server
	 *	space (path must be properly URI encoded and will be normalized and sanitized to the canonical encoding;
	 *	returns a canonically URI encoded path, as translations and cache guarantee to also be properly URI encoded) */
	public makeImmutable(module: string, path: string, options?: { checkFreshness?: boolean }): string {
		const [sanitized, valid] = libHelper.normalizeEncodedPath(path);
		if (!valid)
			this.warning(`Path [${path}] contains malformed URI encoding`);
		return this.makeTranslatedPath(this.cache.immutable(module, sanitized, options));
	}

	/** check if the path relative to the current module is a sub path or the same of the given test base path (can be /base or /base/...) */
	public isSubPathOf(base: string): boolean {
		return libHelper.isSubPath(base, this._path);
	}

	/** check if the path relative to the current module is inside of the given test base path (must be truly inside; /base/...) */
	public isInsideOf(base: string): boolean {
		return libHelper.isInside(base, this._path);
	}

	/** return the remaining path for the sub directory path relative to the current module in base (must be a true sub-directory) */
	public getChildPath(base: string): string {
		return libHelper.childPath(base, this._path);
	}

	/** rebase the path relative to the current module from the old base directory onto the new base (must be a true sub-directory) */
	public getRebased(oldBase: string, newBase: string): string {
		return libHelper.rebasePath(oldBase, newBase, this._path);
	}
}

enum ReceiveState {
	none,
	receiving,
	completed
}
enum UpgradeState {
	none,
	upgrading,
	upgraded
}
enum ResponseState {
	none,
	preparing,
	headerSent,
	completed
}
enum ConnectionState {
	healthy,
	disconnected,
	broken
}

interface HttpResponseInterface {
	setHeader(name: string, value: string): void;
	setStatus(code: number, msg: string): void;
}

class HttpRequestResponse extends libStream.Writable {
	public writer: libStream.Writable;
	public totalSent: number;
	public cache: Buffer | null;
	public status: libBase.StatusType;
	public headers: Record<string, string>;
	public contentSize: number | null;
	public logDetail: string | null;
	public disableEncoding: boolean;
	public contentType: libBase.MediaType;
	public headerSent: boolean;
	public encodingFailed: boolean;
	public responseCompleted: boolean;

	constructor(writer: libStream.Writable, status: libBase.StatusType, headers: Record<string, string>, contentSize: number | null, contentType: libBase.MediaType,
		logDetail: string | null, disableEncoding: boolean, handleData: (chunk: Buffer | null, cb: (err: any) => void) => void, destroy: (err: any, cb: (err: any) => void) => void
	) {
		super({
			write: (chunk, _, cb) => handleData(chunk, cb),
			final: (cb) => handleData(null, cb),
			destroy: (err, cb) => destroy(err, cb)
		});
		this.writer = writer;
		this.totalSent = 0;
		this.cache = null;
		this.status = status;
		this.headers = headers;
		this.contentSize = contentSize;
		this.logDetail = logDetail;
		this.disableEncoding = disableEncoding;
		this.contentType = contentType;
		this.headerSent = false;
		this.encodingFailed = false;
		this.responseCompleted = false;
	}
}

type ClientSocketEvents = { 'data': (data: Buffer) => void, 'close': () => void };

/** look at a response and optionally update or change it (must not perform any asynchronous operations) */
export interface Patcher {
	/** inspect and patch/update actual responses */
	response?: (status: libBase.StatusType, headers: Record<string, string>) => void;

	/** inspect and patch/update html responses (headers might not yet be finalized) */
	html?: (page: libBuilder.HtmlPage, status: libBase.StatusType, headers: Record<string, string>) => void;
}

/** type to invoke to update the logging tag (empty string will hide the tag entry;
 *	null will completely remove the tag; other values will update the tag) */
export type SocketLogTag = (value?: string) => void;

/** cache policy to be applied (Note: if Cache-Control is manually set, it will not be overwritten by the policy) */
export enum CachePolicy {
	/** immutable content that can be cached indefinitely */
	immutable = 'immutable',

	/** static content that may change but can be publically cached */
	static = 'static',

	/** generated content that is private and can change on every request */
	private = 'private',

	/** sensitive content that must not be cached at all */
	sensitive = 'sensitive',

	/** dont configure the cache in any way */
	none = 'none'
}

/**
 *	Does not throw any exceptions, unless explicitly stated.
 *	Http HEAD aware (will silently drain any data sent from a HEAD request).
 *
 *	HTTP version will at least be 1.1 (thereby enforcing host field).
 *	The path will be sanitized and canonically encoded (will always be relative to last translation).
 *	The 'url' component encodes the raw request, without any sanitization.
 *
 *	Path remains URI encoded, as it was received, and path building will use the same encoded paths.
 *	Repeated request responding may override any ongoing responses and may terminate the connection; depending on the prior state.
 *	Not responded to requests will result in [not-found].
 *
 *	Receiving and responding can be started in any order and may overlap (e.g. streaming the upload back out).
 *	As soon as either is started, the request counts as [claimed] and will not be dispatched to any further
 *	modules. A claimed request must be fully completed (response completed, receive consumed, upgrade awaited)
 *	before the module handler returns. Violations abort the pending streams and are answered with an internal
 *	error. Modules own their entire subtree: if a handler returns without claiming the request or responding,
 *	the framework defaults to not-found. Parent modules can intercept any response (including the auto not-found)
 *	via the patch interface.
 *
 *	Receiving data: Will automatically decode the stream and ensure a given maximum is not passed
 *		=> Any errors while receiving will either auto-respond or send the connection into the broken state, and fail the receive reader (stream user does not need to respond).
 *		=> Will terminate a connection, if the upload is not consumed or the client errors.
 *		=> Premature destroying of receive reader will result in the connection being gracefully terminated.
 *		=> All data must have been received before the module handler returns.
 *		=> An 'error' handler must be attached to the returned stream, as the framework will destroy it with an error on broken connections (unhandled stream errors crash the process).
 *	Responding data: Will automatically encode the stream and send the header accordingly
 *		=> Will automatically determine if encoding is to be used
 *		=> Checks if promised number of bytes is provided
 *		=> Only one streamed response can be registered - further attempts fail and abort the first.
 *		=> Will automatically error, if the broken state is detected, and will auto-respond or send the connection into the broken state (stream user does not need to respond).
 *		=> An 'error' handler must be attached to the returned stream, as the framework will destroy it with an error on broken connections (unhandled stream errors crash the process).
 *		=> The stream must be fully processed before the module handler returns, as unfinished responses will otherwise be aborted and answered with an internal error.
 *
 *	A response sent while data is already being streamed (header sent) will break the connection.
 *	A quick response sent while a streamed response has not yet committed its header will cleanly override and abort the stream.
 *	Responses automatically apply the CachePolicy default for the response type, if no other cache control is specified.
 *	Responses will either use the dedicated responder interface and its highWaterMark, or a responder interface, which caches up to socket.highWaterMark.
 *
 *	Upgrade requests, which were not accepted, will be closed after responding.
 *	An accept attempt must be fully awaited before completing the handling procedure.
 *
 *	Defaults [Accept-Ranges] normally to 'none' or to 'bytes' for files
 *	Ensures [Vary] contains 'Accept-Encoding'.
 *	Defaults [Connection] to 'close' for upgrade requests and for some error responses.
 */
export class ClientRequest extends ClientBase {
	private _patcher: { list: Patcher[], index: number | null };
	private _state: {
		respondedPromise: Promise<void>;
		respondedResolve: () => void;
		completedPromise: Promise<void>;
		completedResolve: () => void;
		closedPromise: Promise<void>;
		closedResolve: () => void;
		droppedPromise: Promise<void>;
		droppedResolve: () => void;
		receive: ReceiveState;
		response: ResponseState;
		upgrade: UpgradeState;
		connection: ConnectionState;
		breaking: Promise<void> | null;
		urlMalformed: boolean;
	};
	private _throughput: {
		timer: NodeJS.Timeout | null;
		deadline: number;
		start: number;
		active: boolean;
		busyCheck: (() => boolean)[];
	};
	private _native: {
		response: HttpResponseInterface;
		writer: libStream.Writable;
		socket?: { socket: libStream.Duplex, head: Buffer, wss: libWs.WebSocketServer };
		timeout?: number;
	};
	private _request: libHttp.IncomingMessage;

	private constructor(server: libServer.Server, config: BurntClientConfig, protocol: string, kind: string, request: libHttp.IncomingMessage, response: libHttp.ServerResponse | { socket: libStream.Duplex, head: Buffer, wss: libWs.WebSocketServer }) {
		const url = ClientRequest.parseRequestUrl(protocol, request);
		super(url ?? new libUrl.URL(`${protocol}://_/`), kind, server, config);

		this._patcher = { list: [], index: null };

		let respondedResolve: any = null, completedResolve: any = null, closedResolve: any = null, droppedResolve: any = null;
		this._state = {
			urlMalformed: (url == null),
			respondedPromise: new Promise<void>((resolve) => respondedResolve = resolve),
			respondedResolve: () => { },
			completedPromise: new Promise<void>((resolve) => completedResolve = resolve),
			completedResolve: () => { },
			closedPromise: new Promise<void>((resolve) => closedResolve = resolve),
			closedResolve: () => { },
			droppedPromise: new Promise<void>((resolve) => droppedResolve = resolve),
			droppedResolve: () => { },
			receive: ReceiveState.none,
			response: ResponseState.none,
			upgrade: UpgradeState.none,
			connection: ConnectionState.healthy,
			breaking: null
		};
		this._state.respondedResolve = respondedResolve;
		this._state.completedResolve = completedResolve;
		this._state.closedResolve = closedResolve;
		this._state.droppedResolve = droppedResolve;

		this._request = request;

		/* setup the throughput measurement to detect any stalling connections */
		this._throughput = { timer: null, deadline: 0, start: 0, active: true, busyCheck: [] };
		if (this.config.throughputThreshold > 0) {
			this._throughput.start = Date.now() + this.config.throughputGrace;
			this.updateThroughput(0);
		}

		/* register the necessary network error handlers (only relevant if not already dropped) */
		const handleNetworkEvent = (desc: string) => {
			if (!this.dropped)
				this.dropConnection(ConnectionState.disconnected, (this._state.response == ResponseState.completed ? '' : desc), false, true);
		};
		const lostHandler = () => handleNetworkEvent('Connection lost');
		const closedHandler = () => handleNetworkEvent('Connection closed by remote');
		const timeoutHandler = () => handleNetworkEvent('Connection timed out');
		request.once('error', lostHandler);
		request.once('aborted', closedHandler);
		request.socket.once('timeout', timeoutHandler);
		request.socket.once('error', lostHandler);
		request.socket.once('close', closedHandler);

		/* ensure to remove the events again once the processing has completed */
		this._state.completedPromise.then(() => {
			request.off('error', lostHandler);
			request.off('aborted', closedHandler);
			request.socket.off('timeout', timeoutHandler);
			request.socket.off('error', lostHandler);
			request.socket.off('close', closedHandler);
		});

		/* configure the native interface writer, depending on the actual source parameters */
		let responseWrapper: HttpResponseInterface | null = null;
		let writerWrapper: libStream.Writable | null = null;
		if (response instanceof libHttp.ServerResponse) {
			writerWrapper = response, responseWrapper = {
				setHeader: (name, value) => response.setHeader(name, value),
				setStatus: (code, msg) => { response.statusCode = code; response.statusMessage = msg; }
			};
		}
		else
			[responseWrapper, writerWrapper] = this.wrapSocketWriter(response.socket);
		this._native = { response: responseWrapper, writer: writerWrapper, socket: (response instanceof libHttp.ServerResponse ? undefined : response) };

		/* overwrite the original socket timeout as the client handler will take care of it (set it to twice the conceivable upper bound) */
		this._native.timeout = this._request.socket.timeout;
		this._request.socket.setTimeout((this.config.throughputWindow + this.config.throughputGrace) * 2);
	}

	private static parseRequestUrl(protocol: string, request: libHttp.IncomingMessage): libUrl.URL | null {
		try { return new libUrl.URL(`${protocol}://${request.headers.host?.toLowerCase() ?? '_'}${request.url}`); }
		catch (_) { return null; }
	}
	private failThroughput(): void {
		if (this.config.throughputThreshold <= 0 || !this._throughput.active || this.dropped)
			return;

		/* check if the connection is still considered busy and should receive a grace delay */
		for (const cb of this._throughput.busyCheck) {
			let result = false;
			try { result = cb(); }
			catch (err: any) {
				this.badClientUsage(`Unhandled exception in busy check: ${err.message}`, false);
			}
			if (!result) continue;

			this.trace(`Deferring throughput closing as connection is busy`);
			this._throughput.start = Date.now() + this.config.throughputGrace;
			this.updateThroughput(0);
			this._request.socket.setTimeout((this.config.throughputWindow + this.config.throughputGrace) * 2);
			return;
		}

		this.breakWithResponse(`Throughput below [${this.config.throughputThreshold}] bytes/sec`, false, true, (desc, headers) => this.respondRequestTimeout(desc, { headers }));
	}
	private updateThroughput(delta: number): void {
		if (this._throughput.timer != null)
			clearTimeout(this._throughput.timer);
		this._throughput.timer = null;
		if (this.config.throughputThreshold <= 0 || !this._throughput.active)
			return;
		const _now = Date.now();
		const now = Math.max(_now, this._throughput.start);

		/* shift the deadline according to the bought time by the throughput */
		const bought = (delta / this.config.throughputThreshold) * 1000;
		this._throughput.deadline = now + Math.min(this.config.throughputWindow, Math.max(0, this._throughput.deadline - now) + bought);
		this._throughput.timer = setTimeout(() => this.failThroughput(), this._throughput.deadline - _now);
	}
	private wrapSocketWriter(socket: libStream.Duplex): [HttpResponseInterface, libStream.Writable] {
		let headers: Record<string, string> = {}, status: number = 0, message: string = '';

		/* wrap the response interface to catch the http parameter */
		const response: HttpResponseInterface = {
			setHeader: (name, value) => {
				if (name.match(BAD_HTTP_HEADER_NAME_REGEX))
					throw new Error('Bad Name');
				if (value.match(BAD_HTTP_STRING_REGEX))
					throw new Error('Bad Value');
				headers[name] = value;
			},
			setStatus: (code, msg) => {
				if (msg.match(BAD_HTTP_STRING_REGEX))
					throw new Error('Bad Status');
				status = code;
				message = msg;
			}
		};

		/* setup the buffer flush, which will also write the header out */
		let headerSent = false, buffer: Buffer | null = null, settled = false;
		const flushBuffer = (cb: () => void, last: boolean) => {
			let prefix = '', suffix = '';

			/* check if the header first needs to be sent */
			const chunked = (headerSent || !last);
			if (!headerSent) {
				headerSent = true;

				/* construct the header text */
				if (chunked)
					headers['Transfer-Encoding'] = 'chunked';
				else
					headers['Content-Length'] = (buffer?.byteLength ?? 0).toString();
				prefix = `HTTP/1.1 ${status} ${message}\r\n`;
				for (const key in headers)
					prefix += `${key}: ${headers[key]}\r\n`;
				prefix += '\r\n';
			}

			/* check if a chunk prefix needs to be added and if the chunk suffix needs to be added */
			if (chunked) {
				if (buffer != null)
					prefix += `${buffer.byteLength.toString(16)}\r\n`, suffix += '\r\n';
				if (last)
					suffix += '0\r\n\r\n';
			}

			/* construct the final chunk to be sent */
			let chunks = [];
			if (prefix != '')
				chunks.push(Buffer.from(prefix, 'utf-8'));
			if (buffer != null)
				chunks.push(buffer);
			if (suffix != null)
				chunks.push(Buffer.from(suffix, 'utf-8'));
			const chunk = (chunks.length == 1 ? chunks[0] : Buffer.concat(chunks));

			/* clear the cached data and write the data to the socket (chunk can never be empty, as either a buffer must exist, or the final suffix or header needs to be sent) */
			buffer = null;
			if (last)
				socket.end(chunk, cb);
			else
				socket.write(chunk, cb);
		};

		/* note: writer cannot interfere with later web-socket native socket, as the web-socket will only be instantiated,
		*	if response-state is none, and vice versa, all writers will fail once web-socket has started, as response-state
		*	will be header-sent (writer itself can never fail, and no error handlers will be attached to it; it uses the
		*	underlying sockets watermark as buffering capacity) */
		const writer = new libStream.Writable({
			destroy: (err, cb) => {
				if (settled) return; settled = true;
				socket.destroy(err ?? undefined);
				cb(null);
			},
			write: (chunk, _, cb) => {
				if (settled) return;
				buffer = (buffer == null ? chunk as Buffer : Buffer.concat([buffer, chunk]));
				if (buffer.byteLength < writer.writableHighWaterMark)
					return cb(null);
				flushBuffer(cb, false);
			},
			final: (cb) => {
				if (settled) return; settled = true;
				flushBuffer(cb, true);
			},
			highWaterMark: socket.writableHighWaterMark
		});
		return [response, writer];
	}
	private async killNativeConnection(graceful: boolean): Promise<void> {
		const writer = this._native.writer;

		/* raw closer, which will fully destroy the connection */
		const closeConnection = () => {
			if (this._request.destroyed && writer.destroyed)
				return;
			const error = new Error('Connection broken');

			this._request.destroy(error);
			writer.destroy(error);
		};

		if (!graceful || writer.writableFinished || writer.destroyed)
			return closeConnection();

		/* if graceful, wait for the last queued data to be sent; may be called multiple times */
		return new Promise<void>((resolve) => {
			let settled = false, handler = () => {
				if (settled) return; settled = true;
				closeConnection();
				resolve();
			};
			writer.once('finish', () => handler());
			writer.once('error', () => handler());
			writer.once('close', () => handler());
		});
	}
	private get dropped(): boolean {
		return (this._state.connection != ConnectionState.healthy);
	}
	private dropConnection(state: ConnectionState, reason: string, graceful: boolean, expected: boolean): void {
		/* disconnects and expected breaks (remote-caused or documented cleanup) are logged as
		*	normal logs, only breaks caused by server-side misbehavior are logged as errors */
		if (reason != '') {
			const description = libLog._logs.buildConnectionStatusLog(state == ConnectionState.disconnected, reason);
			if (expected)
				this.log(description);
			else
				this.error(description);
		}

		/* check if the connection has already been dropped, in which
		*	case at most the forceful destruction needs to be applied */
		if (this.dropped) {
			if (!graceful)
				this.killNativeConnection(false);
			return;
		}

		/* resolve breaking before responded/closed to ensure it is delivered first */
		this._state.connection = state;
		this._state.droppedResolve();
		this._state.respondedResolve();
		this._state.closedResolve();

		/* setup the promise beforehand to ensure the promise body does not recursively
		*	enter this handler again, and sees the completed object still being unset */
		let resolver = () => { };
		this._state.breaking = new Promise<void>((res) => resolver = res);

		/* setup the break promise to ensure the connection is killed properly with the given grace */
		(async () => {
			let settled = false;

			const forceDestroy = setTimeout(() => {
				if (settled) return; settled = true;
				this.killNativeConnection(false);
				resolver();
			}, this.config.killGraceTimeout);

			await this.killNativeConnection(graceful);
			clearTimeout(forceDestroy);

			if (settled) return; settled = true;
			resolver();
		})();
	}
	private badClientUsage(reason: string, close: boolean): void {
		this.respondInternalError(`Bad Usage: ${reason}`, (close ? { headers: { 'Connection': 'close' } } : undefined));
	}
	private breakWithResponse(description: string, graceful: boolean, expected: boolean, respond: (description: string, headers: Record<string, string>) => void): void {
		if (!this.dropped && (this._state.response == ResponseState.none || this._state.response == ResponseState.preparing)) {
			respond(description, { 'Connection': 'close' });
			this.dropConnection(ConnectionState.broken, '', true, expected);
		}
		else
			this.dropConnection(ConnectionState.broken, description, graceful, expected);
	}
	private applyCachePolicy(headers: Record<string, string>, should: CachePolicy, override?: CachePolicy): void {
		if ('Cache-Control' in headers)
			return;
		const policy = this.config.cache[override ?? should];
		if (policy != '')
			headers['Cache-Control'] = policy;
	}
	private invokePatcher(cb: (patcher: Patcher) => void): void {
		const index = this._patcher.index;

		/* invoke all registered patcher to let them modify the content (in reverse order to ensure
		*	first added is last executed, and check if one of them produced an alternate response) */
		for (this._patcher.index = (index == null ? this._patcher.list.length : index) - 1; this._patcher.index >= 0; --this._patcher.index) {
			if (this._state.response != ResponseState.none && this._state.response != ResponseState.preparing)
				break;

			try {
				cb(this._patcher.list[this._patcher.index]);
			} catch (err: any) {
				this.badClientUsage(`Unhandled exception in patcher: ${err.message}`, false);
			}
		}
		this._patcher.index = index;
	}

	private closeHeader(status: libBase.StatusType, headers: Record<string, string>, content: { media: libBase.MediaType, size?: number } | null, logDetail: string | null): boolean {
		const logMessage = libLog._logs.buildResponseDescription(status.code, status.msg, (content == null ? null : { size: content.size, type: content.media.mediaType }), logDetail);

		/* configure the default header values (always extend the vary-header by the encoding,
		*	as any custom vary must not suppress it while encoding negotiation is performed) */
		if (!('Accept-Ranges' in headers))
			headers['Accept-Ranges'] = 'none';
		libHelper.extendVaryHeader(headers);
		if (!('Date' in headers))
			headers['Date'] = new Date().toUTCString();
		for (const [key, value] of Object.entries(this.config.commonHeaders)) {
			if (!(key in headers))
				headers[key] = value;
		}
		if (this.config.serverName != '' && !('Server' in headers))
			headers['Server'] = this.config.serverName;
		if (content != null) {
			headers['Content-Type'] = libHelper.buildMediaTypeIdentifier(content.media);
			if (content.size != null)
				headers['Content-Length'] = content.size.toString();
		}

		/* check if its an upgrade request, which is always marked as being closed, as the
		*	underlying web-server will not take it back into the queue for keep-alive sockets */
		if (this._native.socket != null)
			headers['Connection'] = 'close';

		/* invoke all registered patchers to let them update the content */
		this.invokePatcher((patcher) => patcher.response?.(status, headers));
		if (this.dropped || (this._state.response != ResponseState.none && this._state.response != ResponseState.preparing)) {
			this.trace(`Discarding response after patcher ${logMessage}`);
			return false;
		}

		if (status.code >= 500)
			this.error(libLog._logs.buildResponseLog(logMessage));
		else
			this.log(libLog._logs.buildResponseLog(logMessage));

		/* mark the response as determined as it will now be sent this way */
		this._state.response = ResponseState.headerSent;
		this._state.respondedResolve();

		/* setup the response status and headers (guard against invalid header values) */
		try { this._native.response.setStatus(status.code, status.msg); } catch (_) {
			this.dropConnection(ConnectionState.broken, `Failed to finalize response: Bad status message`, false, false);
			return false;
		}
		for (const [key, value] of Object.entries(headers)) {
			try { this._native.response.setHeader(key, value); } catch (_) {
				this.error(`Failed to set header [${key}]: Bad header value`);
			}
		}
		return true;
	}
	private sendFullResponse(status: libBase.StatusType, logDetail: string | null, headers: Record<string, string>, content: { media: libBase.MediaType, body?: Buffer } | null): void {
		/* check if the response can still be sent */
		if (this.dropped || this._state.response == ResponseState.headerSent || this._state.response == ResponseState.completed) {
			const description = libLog._logs.buildResponseDescription(status.code, status.msg, (content == null ? null : { size: content.body?.byteLength, type: content.media.mediaType }), logDetail);

			if (this._state.response == ResponseState.completed)
				return this.warning(`Request already completed, discarding response ${description}`);
			else if (this.dropped)
				return this.trace(`Request broken, discarding response ${description}`);
			return this.dropConnection(ConnectionState.broken, `Overlap with committed response ${description}`, false, false);
		}

		/* check if the response body can be dynamically encoded */
		let encoding: libBase.EncodingType | null = null;
		let contentSize: number | null = content?.body?.byteLength ?? null;
		if (content != null) {
			/* check if the data should be encoded (if the size is not known, pretend the buffer to be large enough) */
			encoding = libHelper.negotiateEncoding(this.headers['accept-encoding'] ?? null, contentSize, content.media);
			if (encoding != null) {
				if (content.body != null)
					content.body = encoding.encodeBuffer(content.body);
				headers['Content-Encoding'] = encoding.name;
			}
		}

		const fullDetail = (encoding == null ? logDetail : (logDetail == null ? 'D' : `${logDetail} d`) + `ynamically [${encoding.name}] encoded from [${contentSize ?? 'unknown'}]`);
		if (!this.closeHeader(status, headers, (content == null ? null : { media: content.media, size: content.body?.byteLength }), fullDetail))
			return;
		this._state.response = ResponseState.completed;

		/* try to finalize the response (can throw an exception for invalid status or header content) */
		try {
			if (!this.isHead && content?.body != null) {
				this.updateThroughput(content.body.length);
				this._native.writer.end(content.body);
			}
			else
				this._native.writer.end();
		} catch (err: any) {
			this.dropConnection(ConnectionState.broken, `Failed to finalize response: ${err.message}`, false, false);
		}
	}
	private sendClientSetupHeader(resp: HttpRequestResponse, chunk: Buffer | null, cb: (err: any) => void): void {
		const last = (chunk == null);
		const cached = (resp.cache != null);

		/* check if previous data were cached and combine them */
		if (resp.cache != null)
			chunk = (chunk == null ? resp.cache : Buffer.concat([resp.cache, chunk]));
		resp.cache = null;

		/* check if the sending should be deferred to determine if compression
		*	should be enabled or to allow inline compression on small packets
		*	(for a head request, dont cache any data, immediately send the header) */
		if (!last && !this.isHead && (chunk!.byteLength < libBase.MIN_ENCODING_SIZE || !cached)) {
			resp.cache = chunk;
			return cb(null);
		}

		/* for a 'HEAD' request, pretend the size to not be known yet, if it has not been explicitly provided */
		let fullContentSize = resp.contentSize;
		if (fullContentSize == null && last && !this.isHead)
			fullContentSize = (chunk?.byteLength ?? 0);

		/* check if the content should be dynamically encoded */
		if (resp.disableEncoding) {
			if (!this.closeHeader(resp.status, resp.headers, { media: resp.contentType, size: fullContentSize ?? undefined }, resp.logDetail))
				return cb(null);
			return this.sendClientWrite(resp, chunk, last, cb);
		}

		/* lookup the dynamic encoder (for [head] and no explicit content, default to size being valid to just
		*	assume an encoding - can always be disabled in the real run, should the data be too short) */
		let encoding = libHelper.negotiateEncoding(this.headers['accept-encoding'] ?? null, fullContentSize ?? chunk?.byteLength ?? null, resp.contentType);
		if (encoding == null) {
			if (!this.closeHeader(resp.status, resp.headers, { media: resp.contentType, size: fullContentSize ?? undefined }, resp.logDetail))
				return cb(null);
			return this.sendClientWrite(resp, chunk, last, cb);
		}
		resp.headers['Content-Encoding'] = encoding.name;
		resp.headers['Accept-Ranges'] = 'none';

		/* for HEAD, clear the content size, as it is not known through the encoder, and for real
		*	requests, check if the header can be encoded inplace (update the content size, as it is now
		*	exact but differs due to being encoded) and otherwise configure the encoding pipeline */
		if (this.isHead)
			fullContentSize = null;
		else if (last) {
			chunk = encoding.encodeBuffer(chunk!);
			resp.contentSize = chunk.byteLength;
			fullContentSize = chunk.byteLength;
		} else {
			fullContentSize = null;
			const encoder = encoding.makeEncode();
			encoder.pipe(resp.writer);
			resp.writer = encoder;

			encoder.once('error', (err: any) => {
				resp.encodingFailed = true;
				resp.destroy(err);
			});
		}

		const logDetail = (resp.logDetail ?? '') + (resp.logDetail == null ? 'D' : ' d') + `ynamically [${encoding.name}] encoded`;
		if (!this.closeHeader(resp.status, resp.headers, { media: resp.contentType, size: fullContentSize ?? undefined }, logDetail))
			return cb(null);
		return this.sendClientWrite(resp, chunk, last, cb);
	}
	private sendClientWrite(resp: HttpRequestResponse, chunk: Buffer | null, last: boolean, cb: (err: any) => void): void {
		/* this point is only reached once the header has been sent */
		resp.headerSent = true;

		/* check if this is a head write, in which case the response can
		*	just be marked as completed, and all other data can be drained */
		if (this.isHead) {
			this._state.response = ResponseState.completed;
			resp.responseCompleted = true;
			resp.writer.end(() => cb(null));
			return;
		}

		/* update the total-sent counter and check if the upper-bound is broken */
		if (chunk != null) {
			this.updateThroughput(chunk.byteLength);
			resp.totalSent += chunk.byteLength;
			if (resp.contentSize != null && resp.totalSent > resp.contentSize) {
				this.badClientUsage('Sent more data than promised', false);
				return cb(new Error('Sent more data than promised'));
			}
		}

		/* check if this is an intermediate write, and write the data out */
		if (!last) {
			resp.writer.write(chunk!, () => cb(null));
			return;
		}

		/* check if all expected data have been provided */
		if (resp.contentSize != null && resp.totalSent < resp.contentSize) {
			this.badClientUsage('Sent fewer data than promised', false);
			return cb(new Error('Sent fewer data than promised'));
		}

		/* mark the state as completed and sent the last package */
		this._state.response = ResponseState.completed;
		resp.responseCompleted = true;
		if (chunk != null)
			resp.writer.end(chunk, () => cb(null));
		else
			resp.writer.end(() => cb(null));
	}
	private sendClientData(status: libBase.StatusType, media: libBase.MediaType, headers: Record<string, string>, disableEncoding: boolean, contentSize: number | null, logDetail: string | null): libStream.Writable {
		const makeErrorStream = (msg: string) => new libStream.Writable({ write(_0, _1, cb) { cb(new Error(msg)) }, final(cb) { cb(new Error(msg)) } });

		/* check if the object is already responded */
		if (this.dropped) {
			this.trace('Request broken, discarding streamed response');
			return makeErrorStream('Connection broken');
		}
		if (this._state.response != ResponseState.none) {
			this.trace('Request already responded, discarding streamed response');
			return makeErrorStream('Connection already responded');
		}
		this._state.response = ResponseState.preparing;

		/* construct the actual response wrapper, which takes care of dynamic encoding and error handling */
		const output = new HttpRequestResponse(this._native.writer, status, headers, contentSize, media, logDetail, disableEncoding,
			(chunk: Buffer | null, cb: (err: any) => void) => {
				/* check if this is a head-request, in which case the data will just be drained */
				if (this.isHead && output.responseCompleted)
					return cb(null);

				/* check if the connection has been marked as failed or completed */
				if (this.dropped)
					return cb(new Error('Connection broken'));
				if (this._state.response == ResponseState.completed)
					return cb(new Error('Responding to completed response'));
				if (this._state.response == ResponseState.headerSent && !output.headerSent)
					return cb(new Error('Responding to claimed connection'));

				/* handle the data accordingly and check for any errors due to malformed headers */
				try {
					if (output.headerSent)
						return this.sendClientWrite(output, chunk, chunk == null, cb);
					this.sendClientSetupHeader(output, chunk, cb);
				}
				catch (err: any) {
					this.dropConnection(ConnectionState.broken, `Failed to process response: ${err.message}`, false, false);
					return cb(new Error('Connection broken'));
				}
			},
			(err: any, cb: (err: any) => void) => {
				/* check if the response was already completed, in which case the error must
				*	be ignored, as the encoder might still contain buffered data to be sent */
				if (output.responseCompleted)
					return cb(err);
				output.responseCompleted = true;

				/* check if the output stream was an encoder, in which case it can be destroyed */
				if (output.writer !== this._native.writer)
					output.writer.destroy();

				/* check if the client was consumed by another response and should not be closed/responded to anymore */
				if (this.dropped || (!output.headerSent && this._state.response != ResponseState.preparing))
					return cb(err);

				/* check if its an encoding failure (header must already have been sent) */
				if (output.encodingFailed)
					this.dropConnection(ConnectionState.broken, `Encoding failure: ${err.message}`, false, false);

				/* forward the module-supplied error and ensure the connection is closed */
				else
					this.breakWithResponse(`Response error: ${err.message}`, false, false, (desc, _) => this.respondInternalError(desc, { headers }));
				return cb(err);
			}
		);

		/* register the handler to detect closed or failed connections */
		this._state.droppedPromise.then(() => output.destroy(new Error('Connection broken')));
		this._state.closedPromise.then(() => output.destroy(new Error('Response no longer writable')));

		/* register the handler to determine if the response has been overshadowed (responded without this headerSent being set) */
		this._state.respondedPromise.then(() => {
			if (!output.headerSent)
				output.destroy(new Error('Response already claimed'));
		});
		return output;
	}
	private receiveClientData(maxLength: number | null): libStream.Readable {
		const makeErrorStream = (msg: string) => new libStream.Readable({ read() { this.destroy(new Error(msg)) } });

		/* check if the object is ready for receiving */
		if (this.dropped) {
			this.trace('Request broken, discarding receive');
			return makeErrorStream('Connection broken');
		}
		if (this._state.receive != ReceiveState.none) {
			this.trace('Request already receiving, discarding additional receive');
			return makeErrorStream('Connection is already being received');
		}
		this._state.receive = ReceiveState.receiving;

		/* setup the accumulation transformer (which will also be returned in the end; mark receiving
		*	as completed upon destroy - will automatically drain the request on cleanup) */
		let accumulated = 0;
		const output = new libStream.Transform({
			transform: (chunk, _, cb) => {
				if (this.dropped)
					return cb(new Error('Connection broken'));

				/* check the maximum count (is violated) */
				this.updateThroughput(chunk.byteLength);
				accumulated += chunk.byteLength;
				if (maxLength == null || accumulated <= maxLength)
					return cb(null, chunk);
				this.respondContentTooLarge(maxLength, accumulated);
				cb(new Error('Request payload is too large'));
			},
			destroy: (err, cb) => {
				if (this._state.receive == ReceiveState.receiving) {
					this._request.unpipe();
					this._state.receive = ReceiveState.completed;
				}
				cb(err);
			},
			final: (cb) => {
				if (this._state.receive == ReceiveState.receiving) {
					this._request.unpipe();
					this._state.receive = ReceiveState.completed;
				}
				cb();
			},
		});

		/* check if the content is encoded and create the chain of decoders (in reverse to ensure the nesting is correct) */
		let stream: libStream.Readable = this._request;
		if (this.headers['content-encoding'] != null) {
			const encodings = libHelper.splitAndTrimList(this.headers['content-encoding'], ',', false);

			for (let i = encodings.length - 1; i >= 0; --i) {
				const encoding = libHelper.lookupEncoding(encodings[i]);

				if (encoding == null) {
					output.destroy();
					this.respondUnsupportedMediaType(encodings[i], libHelper.supportedEncodingNames().join(','));
					return makeErrorStream('Unsupported content encoding');
				}

				/* configure the piping accordingly */
				const decoder = encoding.makeDecode();
				stream = stream.pipe(decoder);
				decoder.once('error', (err: any) => {
					if (!output.destroyed)
						this.respondBadRequest({ reason: 'Invalid data encoding' });
					output.destroy(err);
				});

				/* register the cleanup handler to ensure the decoder is destroyed on completion */
				output.once('close', () => decoder.destroy());
			}
		}

		/* check if too many data have been promised (cannot be trusted if content-encoding is enabled) */
		else if (maxLength != null && this.headers['content-length'] != null) {
			const contentSize = parseInt(this.headers['content-length']);

			/* check if the length is valid and otherwise mark the request as 'consumed' */
			if (!isFinite(contentSize) || contentSize < 0 || contentSize > maxLength) {
				output.destroy();
				this.respondContentTooLarge(maxLength, contentSize);
				return makeErrorStream('Request payload is too large');
			}
		}

		/* register the handler to detect closed or failed connections */
		this._state.droppedPromise.then(() => output.destroy(new Error('Connection broken')));
		this._state.closedPromise.then(() => output.destroy(new Error('Receive no longer available')));

		/* create the plumbing between stream and output (errors are already handled) */
		return stream.pipe(output);
	}
	private hasDataQueued(): boolean {
		const request = this._request;

		/* check if data remain in the pipeline, or any data should be uploaded */
		if (request.readableEnded || request.destroyed)
			return false;
		const length = parseInt(this.headers['content-length'] ?? '0');
		const chunked = (this.headers['transfer-encoding'] != null);
		return (length > 0 || chunked);
	}
	private drainQueuedData(): Promise<void> {
		let settled = false;
		return new Promise<void>((resolve) => {
			this.trace('Draining uploaded data');

			/* await the receiving of all data or the connection breaking */
			const cb = () => {
				if (settled) return; settled = true;
				this._request.removeListener('end', cb);
				resolve();
			};
			this._request.once('end', cb);
			this._state.droppedPromise.then(cb);
			this._state.closedPromise.then(cb);

			/* ensure the throughput is still updated and ensure the reuqest is in flow state */
			this._request.on('data', (chunk: Buffer) => this.updateThroughput(chunk.byteLength));
			this._request.resume();
		});
	}
	private cleanupContext(finalize: boolean): void {
		const handleFailure = (reason: string) => {
			/* close after breaking to ensure breaking is delivered first */
			if (finalize)
				this.dropConnection(ConnectionState.broken, reason, false, false);
			else
				this.badClientUsage(reason, true);
			this._state.closedResolve();
		};

		/* ensure the connection is default replied with not-found */
		if (!this.dropped && this._state.response == ResponseState.none)
			this.respondNotFound();

		/* ensure that the data have been fully received */
		if (!this.dropped && this._state.receive == ReceiveState.receiving)
			handleFailure('Receive stream not consumed');

		/* check if the upgrade was not fully awaited */
		if (!this.dropped && this._state.upgrade == UpgradeState.upgrading)
			handleFailure('Upgrade not fully awaited');

		/* ensure that the response was properly sent */
		if (!this.dropped && this._state.response != ResponseState.completed)
			handleFailure('Response not completed');
	}

	public _pushContext(map: Record<string, string | null> | null, identity: string): ClientContext | null {
		let sanitized: Record<string, string | null> | null = null;
		let match: [string, string | null] | null = null;

		/* check if this is only an identity map, in which case nothing complex needs to be evaluated */
		if (map == null || Object.keys(map).length == 1 && map['/'] == '/')
			match = ['/', '/'];

		/* create the merged reverse map and check if the map applies to the current translation */
		else {
			sanitized = {};
			for (const [_from, _to] of Object.entries(map)) {
				/* normalize and sanitize the encoding of the mapping (to ensure matching against the canonically encoded paths is consistent) */
				const [from, validFrom] = libHelper.normalizeEncodedPath(_from);
				const [to, validTo]: [string | null, boolean] = (_to == null ? [null, true] : libHelper.normalizeEncodedPath(_to));
				if (!validFrom || !validTo) {
					this.warning(`Translation [${_from}] => [${_to}] contains malformed URI encoding`);
					continue;
				}
				sanitized[from] = to;

				/* check if the mapping can be applied to the current path */
				if (this.isSubPathOf(from) && (match == null || match[0].length < from.length))
					match = [from, to];
			}
			if (match == null || match[1] == null)
				return null;
		}

		const current = new ClientContext(this._path, this.identity, this._translation.length,
			this._throughput.busyCheck.length, this._patcher.list.length);

		/* setup the new path, all path translations, and the tagged logging identity */
		this._path = libHelper.rebasePath(match[0], match[1]!, this._path);
		if (sanitized != null)
			this._translation.push(sanitized);
		if (identity != '')
			this.logSetIdentity(`${this.identity}.${identity}`);
		return current;
	}
	public _popContext(snapshot: ClientContext): void {
		this.cleanupContext(false);

		/* restore the context */
		this._path = snapshot.path;
		this.logSetIdentity(snapshot.identity);
		this._translation.splice(snapshot.translationCount);
		this._throughput.busyCheck.splice(snapshot.busyCount);
		this._patcher.list.splice(snapshot.patchCount);
	}
	public static _fromRequest(protocol: string, request: libHttp.IncomingMessage, response: libHttp.ServerResponse, config: BurntClientConfig, server: libServer.Server): ClientRequest {
		return new ClientRequest(server, config, protocol, 'request', request, response);
	}
	public static _fromUpgrade(protocol: string, request: libHttp.IncomingMessage, socket: libStream.Duplex, head: Buffer, config: BurntClientConfig, server: libServer.Server, wss: libWs.WebSocketServer): ClientRequest {
		return new ClientRequest(server, config, protocol, 'upgrade', request, { socket, head, wss });
	}
	public _initializeConnection(): boolean {
		/* validate the HTTP version (to ensure a host value is provided) */
		if (this._request.httpVersion == '1.0') {
			this.respondHttpVersionNotSupported('1.1');
			return false;
		}

		/* validate that the request url could be constructed */
		if (this._state.urlMalformed) {
			this.respondBadRequest({ reason: 'Malformed request url or host header' });
			return false;
		}

		/* normalize the path encoding and validate it */
		const [normalized, valid] = libHelper.normalizeEncodedPath(this._path);
		if (!valid)
			this.respondBadRequest({ reason: 'Malformed URI encoding in path' });
		else
			this._path = normalized;
		return valid;
	}
	public async _finalizeConnection(): Promise<void> {
		this.cleanupContext(true);

		/* check if data remain in the pipeline, in which case they either need to be consumed, or the
		*	connection needs to be closed to ensure the sender does not pipe more data over (dropped
		*	connections have already been cleaned up and cannot receive the data anymore anyways) */
		if (this.hasDataQueued() && !this.dropped) {
			if (this.config.drainUpload)
				await this.drainQueuedData();
			else
				this.dropConnection(ConnectionState.broken, (this._request.readableLength > 0 ? 'Uploaded data not consumed' : 'Potential uploaded data not consumed'), true, false);
		}

		/* check if the connection was an upgrade but was not was accepted, which needs to be
		*	killed, as the underlying web-server will not clean this connection up anymore */
		if (this._native.socket != null && this._state.upgrade != UpgradeState.upgraded && !this.dropped)
			this.dropConnection(ConnectionState.broken, 'Upgrade was not accepted', true, true);

		/* kill the throughput timer, as it either does not need to be checked anymore, or it
		*	will have left the connection as broken, and will automatically be closed now */
		if (this._throughput.timer != null)
			clearTimeout(this._throughput.timer);
		this._throughput.timer = null;
		this._throughput.active = false;

		/* check if the connection was dropped and await its grace cleanup completion */
		if (this.dropped)
			await this._state.breaking!;

		/* recover the original socket timeout (not for sockets, as they take care of the timeout themselves) */
		if (this._state.upgrade != UpgradeState.upgraded || this._state.response != ResponseState.completed)
			this._request.socket.setTimeout(this._native.timeout ?? 0);
		this._state.completedResolve();
	}

	/** respond with an internal error and kill the connection (if [gracefully], give previous responses time to be received) */
	public killConnection(reason: string, options?: { graceful?: boolean }): void {
		/* if the response is already completed/broken, treat is as a friendly closing */
		if (this._state.response == ResponseState.completed || this.dropped)
			return this.dropConnection(ConnectionState.broken, `Connection killed: ${reason}`, true, true);
		this.breakWithResponse(`Connection killed: ${reason}`, (options?.graceful ?? false), false, (desc, headers) => this.respondInternalError(desc, { headers }));
	}

	/** request has been engaged with (receiving or responding was started, or the connection has been
	 *	dropped); a claimed request will not be dispatched to further modules and the framework will default
	 *	to not-found if the handler returns without claiming or responding */
	public get claimed(): boolean {
		return (this._state.response != ResponseState.none || this._state.receive != ReceiveState.none || this.dropped);
	}

	/** resolves whenever the response has been determined (a response header has been sent or the connection was
	 *	dropped; must only be awaited while a response can still be produced elsewhere, such as by concurrently
	 *	dispatched modules or background tasks - awaiting it as the sole handler without responding will stall
	 *	the request until the connection is closed externally) */
	public get responded(): Promise<void> {
		return this._state.respondedPromise;
	}

	/** resolves whenever the request has been fully processed (will first resolve after all handlers of the request
	 *	have returned; must not be awaited from within any handler of the request itself - not even after fully
	 *	responding - as this will lead to deadlocks) */
	public get completed(): Promise<void> {
		return this._state.completedPromise;
	}

	/** http request headers */
	public get headers(): libHttp.IncomingHttpHeaders {
		return this._request.headers;
	}

	/** http request method */
	public get method(): string {
		return this._request.method ?? '';
	}

	/** was the http request a head request */
	public get isHead(): boolean {
		return (this._request.method == 'HEAD');
	}

	/** address of connection remote */
	public get remote(): { address?: string, family?: string, port?: number } {
		return { address: this._request.socket.remoteAddress, port: this._request.socket.remotePort, family: this._request.socket.remoteFamily };
	}

	/** return the string formatted media-type (or empty string for no media type) */
	public getMediaType(): string {
		const type = libHelper.splitAndTrimList(this.headers['content-type'] ?? null, ';', true)[0] ?? '';
		return type.toLowerCase();
	}

	/** check the content-type for a media-type and otherwise return the default type */
	public getMediaTypeCharset(defEncoding: string): string {
		const type = this.headers['content-type'];
		if (type == null)
			return defEncoding;

		/* look for the first charset entry in the content-type list */
		for (const part of libHelper.splitAndTrimList(type, ';', true)) {
			if (part.substring(0, 8).toLowerCase() != 'charset=')
				continue;
			let value = part.substring(8).trim();

			/* remove the potential quotes around the charset value */
			const quoted = value.startsWith('"');
			if (quoted != value.endsWith('"'))
				break;
			if (quoted)
				value = value.substring(1, value.length - 1).trim();

			if (value.length == 0)
				break;
			return value.trim().toLowerCase();
		}
		return defEncoding;
	}

	/** ensure the media-type is one of the list and otherwise return null and auto-respond with [unsupported-media-type] (defaults to first type, if [noneIsFirst]) */
	public requireMediaType(types: libBase.MediaType[] | libBase.MediaType, options?: { noneIsFirst?: boolean, headers?: Record<string, string> }): libBase.MediaType | null {
		if (!Array.isArray(types))
			types = [types];

		const type = this.getMediaType();
		if (type == '' && options?.noneIsFirst === true)
			return types[0];

		for (let i = 0; i < types.length; ++i) {
			if (type === types[i].mediaType)
				return types[i];
		}
		this.respondUnsupportedMediaType(type, types.map(t => t.mediaType).join(','), options);
		return null;
	}

	/** ensure the method is one of the list and otherwise return null and auto-respond with [method-not-allowed]
	 *	if [headExplicit] is false, method will substitute HEAD for GET, framework will consume the remaining body */
	public requireMethod(methods: string[] | string, options?: { headExplicit?: boolean, headers?: Record<string, string> }): string | null {
		if (!Array.isArray(methods))
			methods = [methods];

		if (methods.indexOf(this.method) >= 0)
			return this.method;

		/* check if the HEAD can be converted to a GET */
		const swapAllowed = (options?.headExplicit !== true && methods.indexOf('GET') >= 0 && methods.indexOf('HEAD') < 0);
		if (this.isHead && swapAllowed)
			return 'GET';

		const allowed = methods.join(',') + (swapAllowed ? ',HEAD' : '');
		this.respondMethodNotAllowed(this.method, allowed, options);
		return null;
	}

	/** register a callback to check if the request is still being processed (delays throughput
	 *	termintion and resets connection timeout; will only be considered within this handler context) */
	public busyCheck(cb: () => boolean): void {
		this._throughput.busyCheck.push(cb);
	}

	/** register a callback to be invoked once the response is concrete, to adjust the headers to be
	 *	sent, or change/add to the response (will only be considered within this handler context) */
	public patch(patcher: Patcher): void {
		this._patcher.list.push(patcher);
	}

	/** respond with [internal-error] and a default text response (reason is logged server-side only) or a custom text response; cache policy defaults to [sensitive] */
	public respondInternalError(reason: string, options?: { message?: string, headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		this.applyCachePolicy(header, CachePolicy.sensitive, options?.cache);

		const content: string = (options?.message ?? `An internal server error occurred while processing the request for [${this.url.pathname}].`);
		this.sendFullResponse(libBase.Status.InternalError, `Reason (not sent): ${reason}`, header, {
			media: libBase.Media.Text, body: Buffer.from(content, 'utf-8')
		});
	}

	/** respond with [forbidden] and a default text response (reason will be logged server-side only) or a custom text response; cache policy defaults to [sensitive] */
	public respondForbidden(options?: { reason?: string, message?: string, headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		this.applyCachePolicy(header, CachePolicy.sensitive, options?.cache);

		const logDetail = (options?.reason != null ? `Reason (not sent): ${options.reason}` : options?.message ?? null);
		const content = (options?.message ?? `Access to [${this.url.pathname}] denied.`);
		this.sendFullResponse(libBase.Status.Forbidden, logDetail, header, { media: libBase.Media.Text, body: Buffer.from(content, 'utf-8') });
	}

	/** respond with a any response of the given configuration (defaults to media-type: text/unknown/-, status: ok); if [lightResponse], the
	 *	content length is suppressed for head responses (to accommodate short-circuiting responding); cache policy defaults to [private] */
	public respond(content: string | Buffer | null, options?: { media?: libBase.MediaType, status?: libBase.StatusType, headers?: Record<string, string>, lightResponse?: boolean, cache?: CachePolicy }): void {
		const status = options?.status ?? libBase.Status.Ok;
		const header = { ...options?.headers };
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		if (content == null)
			return this.sendFullResponse(status, 'no body', header, null);

		let media = options?.media ?? libBase.Media.Text;
		if (typeof content == 'string')
			content = Buffer.from(content, 'utf-8');
		else if (options?.media == null)
			media = libBase.Media.Unknown;

		this.sendFullResponse(status, `[${media.mediaType}] of size [${content.byteLength}]`, header, {
			media, body: ((options?.lightResponse && this.isHead) ? undefined : content)
		});
	}

	/** respond with [bad-request] and a default response (with embedded reason) or a custom text response (message replaces the body and the reason is then logged server-side only); cache policy defaults to [private] */
	public respondBadRequest(options?: { reason?: string, message?: string, headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		const content: string = (options?.message ?? `Request for [${this.url.pathname}] is perceived as malformed${options?.reason == null ? '.' : `:\n${options.reason}`}`);
		this.sendFullResponse(libBase.Status.BadRequest, (options?.reason ?? options?.message ?? null), header, {
			media: libBase.Media.Text, body: Buffer.from(content, 'utf-8')
		});
	}

	/** respond with [conflict] and a default response (with embedded reason) or a custom text response (message replaces the body and the reason is then logged server-side only); cache policy defaults to [private] */
	public respondConflict(options?: { reason?: string, message?: string, headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		const content: string = (options?.message ?? `Conflict for resource [${this.url.pathname}]${options?.reason == null ? '.' : `:\n${options.reason}`}`);
		this.sendFullResponse(libBase.Status.Conflict, (options?.reason ?? options?.message ?? null), header, {
			media: libBase.Media.Text, body: Buffer.from(content, 'utf-8')
		});
	}

	/** respond with [ok] and a default response (with embedded reason) or a custom text response (message replaces the body and the reason is then logged server-side only); cache policy defaults to [private] */
	public respondOk(options?: { reason?: string, message?: string, headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		const content: string = (options?.message ?? `${this.method} was successful for [${this.url.pathname}]${options?.reason == null ? '.' : `:\n${options.reason}`}`);
		this.sendFullResponse(libBase.Status.Ok, (options?.reason ?? options?.message ?? null), header, {
			media: libBase.Media.Text, body: Buffer.from(content, 'utf-8')
		});
	}

	/** respond with [not-found] and a default response or a custom text response; cache policy defaults to [private] */
	public respondNotFound(options?: { message?: string, headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		const content: string = (options?.message ?? `Resource [${this.url.pathname}] could not be found.`);
		this.sendFullResponse(libBase.Status.NotFound, (options?.message ?? null), header, {
			media: libBase.Media.Text, body: Buffer.from(content, 'utf-8')
		});
	}

	/** respond with [created] and a default response or a custom text response; [target]
	 *	must be a well formatted URI encoded string; cache policy defaults to [private] */
	public respondCreated(target: string, options?: { message?: string, headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		header['Location'] = target;
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		const content: string = (options?.message ?? `Resource [${this.url.pathname}] successfully created:\n${target}`);
		this.sendFullResponse(libBase.Status.Created, (options?.message == null ? target : `[${target}]: ${options.message}`), header, {
			media: libBase.Media.Text, body: Buffer.from(content, 'utf-8')
		});
	}

	/** respond with [http-version not supported] and a default text response; cache policy defaults to [private] */
	public respondHttpVersionNotSupported(minVersion: string, options?: { headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		this.sendFullResponse(libBase.Status.HttpVersionNotSupported, minVersion, header, {
			media: libBase.Media.Text, body: Buffer.from(`Resource [${this.url.pathname}] requires at least [${minVersion}].`, 'utf-8')
		});
	}

	/** respond with [not-modified] and no body (ensure the etag and/or last-modified is set); cache policy defaults to [private] */
	public respondNotModified(options?: { etag?: string, lastModified?: string, headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		if (options?.etag != null && !('ETag' in header))
			header['ETag'] = options.etag;
		if (options?.lastModified != null && !('Last-Modified' in header))
			header['Last-Modified'] = options.lastModified;
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		this.sendFullResponse(libBase.Status.NotModified, null, header, null);
	}

	/** respond with [precondition-failed] and a default text response (ensure the etag and/or last-modified is set); cache policy defaults to [private] */
	public respondPreconditionFailed(reason: string, options?: { etag?: string, lastModified?: string, headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		if (options?.etag != null && !('ETag' in header))
			header['ETag'] = options.etag;
		if (options?.lastModified != null && !('Last-Modified' in header))
			header['Last-Modified'] = options.lastModified;
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		this.sendFullResponse(libBase.Status.PreconditionFailed, reason, header, {
			media: libBase.Media.Text, body: Buffer.from(`Precondition for resource [${this.url.pathname}] failed:\n${reason}`, 'utf-8')
		});
	}

	/** respond with [range-not-satisfiable] and a default text response; cache policy defaults to [private] */
	public respondRangeIssue(range: string, size: number, options?: { headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		header['Content-Range'] = `bytes */${size}`;
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		this.sendFullResponse(libBase.Status.RangeIssue, `[${range}] cannot be satisfied for size [${size}]`, header, {
			media: libBase.Media.Text, body: Buffer.from(`Range [${range}] cannot be satisfied for [${this.url.pathname}] of size ${size}.`, 'utf-8')
		});
	}

	/** respond with [unsupported-media-type] and a default text response; cache policy defaults to [private] */
	public respondUnsupportedMediaType(used: string, allowed: string, options?: { headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		this.sendFullResponse(libBase.Status.UnsupportedMediaType, `Allowed was [${allowed}] but [${used}] was used`, header, {
			media: libBase.Media.Text, body: Buffer.from(`Media type [${used}] not supported for [${this.url.pathname}].\nAllowed: ${allowed}`, 'utf-8')
		});
	}

	/** respond with [method-not-allowed] and a default text response; cache policy defaults to [private] */
	public respondMethodNotAllowed(method: string, allowed: string, options?: { headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		header['Allow'] = allowed;
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		this.sendFullResponse(libBase.Status.MethodNotAllowed, `Allowed was [${allowed}] but [${method}] was used`, header, {
			media: libBase.Media.Text, body: Buffer.from(`Method ${method} not allowed for [${this.url.pathname}].\nAllowed: ${allowed}.`, 'utf-8')
		});
	}

	/** respond with [request-timeout] and a default text response; cache policy defaults to [private] */
	public respondRequestTimeout(reason: string, options?: { headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		header['Connection'] = 'close';
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		this.sendFullResponse(libBase.Status.RequestTimeout, reason, header, {
			media: libBase.Media.Text, body: Buffer.from(`Request processing of [${this.url.pathname}] timed out:\n${reason}`, 'utf-8')
		});
	}

	/** respond with [content-too-large] and a default text response; cache policy defaults to [private] */
	public respondContentTooLarge(allowed: number, atLeastProvided: number, options?: { headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		this.sendFullResponse(libBase.Status.ContentTooLarge, `[${atLeastProvided}] > [${allowed}]`, header, {
			media: libBase.Media.Text, body: Buffer.from(`Content of at least size ${atLeastProvided} too large for [${this.url.pathname}].\nAt most ${allowed} bytes are allowed.`, 'utf-8')
		});
	}

	/** respond with [update-required] and a default text response; cache policy defaults to [private] */
	public respondUpdateRequired(upgrade: string, options?: { headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		if (!('Connection' in header))
			header['Connection'] = 'upgrade';
		header['Upgrade'] = upgrade;
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		this.sendFullResponse(libBase.Status.UpgradeRequired, `Required: ${upgrade}`, header, {
			media: libBase.Media.Text, body: Buffer.from(`Endpoint [${this.url.pathname}] requires an upgrade.\nRequired: ${upgrade}`, 'utf-8')
		});
	}

	/** respond with [see-other] to the given target and a default text response (forces method GET);
	 *	[target] must be a well formatted URI encoded string; cache policy defaults to [private] */
	public respondSeeOther(target: string, options?: { headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		header['Location'] = target;
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		this.sendFullResponse(libBase.Status.SeeOther, target, header, {
			media: libBase.Media.Text, body: Buffer.from(`Continue at: ${target}`, 'utf-8')
		});
	}

	/** respond with [temporary-redirect] to the given target and a default text response (preserves method);
	 *	[target] must be a well formatted URI encoded string; cache policy defaults to [private] */
	public respondTemporaryRedirect(target: string, options?: { headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		header['Location'] = target;
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		this.sendFullResponse(libBase.Status.TemporaryRedirect, target, header, {
			media: libBase.Media.Text, body: Buffer.from(`Resource [${this.url.pathname}] temporarily redirects to:\n${target}`, 'utf-8')
		});
	}

	/** respond with [permanent-redirect] to the given target and a default text response (preserves method);
	 *	[target] must be a well formatted URI encoded string; cache policy defaults to [private] */
	public respondPermanentRedirect(target: string, options?: { headers?: Record<string, string>, cache?: CachePolicy }): void {
		const header = { ...options?.headers };
		header['Location'] = target;
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		this.sendFullResponse(libBase.Status.PermanentRedirect, target, header, {
			media: libBase.Media.Text, body: Buffer.from(`Resource [${this.url.pathname}] permanently redirects to:\n${target}`, 'utf-8')
		});
	}

	/** respond with html, can be built on by parent modules, sent once the request has been fully processed (default status is ok;
	 *	for HEAD builds, no actual content will be constructed, nor will its size be estimated); cache policy defaults to [private] */
	public respondHtml(page: libBuilder.HtmlPage, options?: { status?: libBase.StatusType, headers?: Record<string, string>, cache?: CachePolicy }): void {
		const status = (options?.status ?? libBase.Status.Ok);
		const header = { ...options?.headers };
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		/* invoke all registered html patchers to let them update the content */
		this.invokePatcher((patcher) => patcher.html?.(page, status, header));

		/* mark first as completed now */
		const content = (this.isHead ? undefined : Buffer.from(page.finalize(), 'utf-8'));
		this.sendFullResponse(status, `Built HTML${this.isHead ? ' light-response' : ''}`, header, {
			media: libBase.Media.Html, body: content
		});
	}

	/** respond with the value encoded as json; if [isJson], the content is expected to be a valid json string (default status is ok;
	 *	for HEAD builds, no actual content will be constructed, nor will its size be estimated); cache policy defaults to [private] */
	public respondJson(value: any, options?: { status?: libBase.StatusType, headers?: Record<string, string>, cache?: CachePolicy, isJson?: boolean }): void {
		const status = options?.status ?? libBase.Status.Ok;
		const header = { ...options?.headers };
		this.applyCachePolicy(header, CachePolicy.private, options?.cache);

		const content = (this.isHead ? undefined : Buffer.from((options?.isJson ? (value as string) : JSON.stringify(value)), 'utf-8'));
		this.sendFullResponse(status, `Created JSON${this.isHead ? ' light-response' : ''}`, header, {
			media: libBase.Media.Json, body: content
		});
	}

	/** [no-throw but errors, register 'error' handler] send data with [media type] and [status] and return a writable stream (default: status is ok, media is
	 *	unknown, disableEncoding is false); if a content size is provided, stream expects exactly this amount of bytes; if [disableEncoding], the encoder will not
	 *	be dynamically negotiated based on the content; for a HEAD request, no lengths will be verified, and the written data will just be drained (can immediately
	 *	be ended using '.end()'); cache policy defaults to [private]; errors will automatically close the client connection properly */
	public respondData(options?: { status?: libBase.StatusType, media?: libBase.MediaType, contentSize?: number, disableEncoding?: boolean, headers?: Record<string, string>, cache?: CachePolicy }): libStream.Writable {
		const status: libBase.StatusType = options?.status ?? libBase.Status.Ok;
		const headers = { ...options?.headers };
		this.applyCachePolicy(headers, CachePolicy.private, options?.cache);

		return this.sendClientData(status, options?.media ?? libBase.Media.Unknown, headers, options?.disableEncoding ?? false, options?.contentSize ?? null, 'Application data');
	}

	/** try to respond with the given file, return false, if the file does not exist (range aware, HEAD aware); specify [checkFreshness] to re-validate the file stats
	 *	on disk before serving from cache; the media type can be overwritten (defaults to extracting media-type from the file-path); [encoding] describes the encoding
	 *	of a pre-encoded file (warning: no checks against accepted encodings performed!); status will be [Ok], [partial-content], [not-modified] or according errors
	 *	cache aware and etag/last-modified aware; cache policy defaults to [immutable] for versioned paths, or [static] otherwise; immutable paths must be created
	 *	relative to the modules path space (i.e. before ClientRequest.makePath) and from properly URI encoded paths, as outdated id's are redirected via makePath */
	public async tryRespondFile(filePath: string, options?: { encoded?: string, media?: libBase.MediaType, headers?: Record<string, string>, checkFreshness?: boolean, cache?: CachePolicy }): Promise<boolean> {
		if (options == null)
			options = {};

		/* read the entry from the cache and check if it has been permanently moved and apply the move */
		let cached: libCache.Cached | string | null = null;
		try {
			cached = this.cache.fetchImmutable(filePath, { checkFreshness: options.checkFreshness });
			if (cached == null)
				return false;
		}
		catch (err: any) {
			this.respondInternalError(`Failed to read file [${filePath}]: ${err.message}`);
			return true;
		}
		if (typeof cached == 'string') {
			this.respondPermanentRedirect(this.makePath(cached));
			return true;
		}

		/* parse the range and ensure that its well formed */
		const range = libHelper.parseRangeHeader(this.headers.range ?? null, cached.fileSize());
		if (range.state == libHelper.RangeState.malformed) {
			this.respondBadRequest({ reason: `Issues while parsing http-header range: [${this.headers.range}]` });
			return true;
		}
		else if (range.state == libHelper.RangeState.issue) {
			this.respondRangeIssue(this.headers.range!, cached.fileSize());
			return true;
		}

		/* update the cached reader to read the encoded content (no encoding if already encoded or a range request has occurred,
		*	as the encoded byte representation might not be stable; this is also the reason why the e-tag must be forced to weak,
		*	as the content cannot be guaranteed to be stable across cache flushes or reloads) */
		const media = (options.media ?? libHelper.lookupMediaTypeFromFile(filePath) ?? libBase.Media.Unknown);
		let dynamicEncoder = ((options.encoded != null || range.state != libHelper.RangeState.noRange) ? null : libHelper.negotiateEncoding(this.headers['accept-encoding'] ?? null, cached.fileSize(), media));
		let reader: null | libCache.EncodedCache = null;
		if (dynamicEncoder != null)
			reader = cached.encoded(dynamicEncoder);

		/* mark byte-ranges to be supported in principle and add the caching properties */
		const headers = { ...options.headers };
		const etag = `${(dynamicEncoder != null) ? 'W/' : ''}"${cached.uniqueId()}"`;
		if (dynamicEncoder != null || options.encoded != null)
			headers['Content-Encoding'] = dynamicEncoder?.name ?? options.encoded!;
		headers['Accept-Ranges'] = (dynamicEncoder != null ? 'none' : 'bytes');
		headers['Last-Modified'] = cached.lastModified();
		headers['ETag'] = etag;
		this.applyCachePolicy(headers, (cached.isImmutable() ? CachePolicy.immutable : CachePolicy.static), options?.cache);

		/* validate the conditions (e-tag more relevant than last-modified; invalid times are not
		*	considered errors; no need to set etag/last-modified, as they are already set) */
		if (this.headers['if-match'] != null) {
			if (!libHelper.etagMatchesList(etag, this.headers['if-match'], true)) {
				this.respondPreconditionFailed(`New etag [${etag}]`, { headers });
				return true;
			}
		}
		else if (this.headers['if-unmodified-since'] != null) {
			const result = libHelper.timestampCompare(cached.lastModified(), this.headers['if-unmodified-since']);
			if (result != null && result > 0) {
				this.respondPreconditionFailed(`Modified at [${cached.lastModified()}]`, { headers });
				return true;
			}
		}

		/* check if the response can be skipped due to the resource not having been modified since
		*	the last fetch (etag outweighs last-modified; invalid times are not considered errors) */
		if (this.headers['if-none-match'] != null) {
			if (libHelper.etagMatchesList(etag, this.headers['if-none-match'], false)) {
				this.respondNotModified({ headers });
				return true;
			}
		}
		else if (this.headers['if-modified-since'] != null) {
			const result = libHelper.timestampCompare(cached.lastModified(), this.headers['if-modified-since']);
			if (result != null && result <= 0) {
				this.respondNotModified({ headers });
				return true;
			}
		}

		/* check if the file is empty (can only happen for unused ranges, which would otherwise have issues) */
		if ((reader == null ? cached.fileSize() : reader.contentSize()) === 0) {
			this.sendFullResponse(libBase.Status.Ok, libLog._logs.buildFileEmpty(filePath), headers, { media, body: Buffer.alloc(0) });
			return true;
		}
		if (range.state == libHelper.RangeState.valid)
			headers['Content-Range'] = `bytes ${range.first}-${range.last}/${cached.fileSize()}`;
		const status = (range.state == libHelper.RangeState.noRange ? libBase.Status.Ok : libBase.Status.PartialContent);

		/* create the source stream of the file eagerly, to ensure creation failures can still be answered with a proper
		*	response (not for HEAD requests, as no content is produced; a file having vanished since the cache lookup counts
		*	as not-found; the stream is guaranteed to be consumed or destroyed by the pipeline further down) */
		let source: libStream.Readable | null = null;
		if (!this.isHead) {
			try {
				source = (reader != null ? reader.stream({ eager: true }) : cached.stream({ start: range.first, end: range.last, eager: true }));
			} catch (err: any) {
				if (err.code == 'ENOENT')
					return false;
				this.respondInternalError(`Failed to open file [${filePath}]: ${err.message}`);
				return true;
			}
		}

		/* create the writer stream (doesn't throw, but errors; enforce the selected encoder) */
		let logDetail = libLog._logs.buildFileDetail(range.first, range.last, cached.fileSize(), filePath);
		if (reader != null) {
			logDetail += ` encoded using [${dynamicEncoder!.name}]`;
			if (reader.contentSize() != null)
				logDetail += ` from cache`;
		}
		const stream = this.sendClientData(status, media, headers, true, (reader == null ? range.last - range.first + 1 : reader.contentSize()), logDetail);

		/* check if this is a head request, in which case the stream can just immediately be closed again, to prevent
		*	the file from consuming resources (null-catch any errors to ensure they are not propagated out of the connection) */
		if (this.isHead) {
			stream.once('error', (err: any) => {
				if (err != null)
					this.error(`Failed to stream file [${filePath}]: ${err.message}`);
			});
			await new Promise<void>((resolve) => stream.end(() => resolve()));
			return true;
		}

		/* stream the file content out (open failures have already been handled at creation; remaining stream errors are rare and can only break the connection) */
		await new Promise<void>((resolve) => libStream.pipeline(source!, stream, (err: any) => {
			if (err != null)
				this.error(`Failed to stream file [${filePath}]: ${err.message}`);
			resolve();
		}));
		return true;
	}

	/** [no-throw but errors, register 'error' handler] receive the payload of given max length as a readable stream; automatically responds with given exceptions
	 *	if the payload cannot be received properly; automatically drained if the readable stream is destroyed before reading all data (does not result in an
	 *	error or logs); an 'error' handler must be attached to the returned stream, as the framework will destroy it with an error on broken connections */
	public receiveData(maxLength: number | null): libStream.Readable {
		return this.receiveClientData(maxLength);
	}

	/** [throws] receive the payload of given max length as a single complete buffer
	 *	automatically responds with given exceptions if the payload cannot be received properly */
	public receiveAllBuffer(maxLength: number | null): Promise<Buffer> {
		return new Promise((resolve, reject) => {
			let stream: libStream.Readable = this.receiveClientData(maxLength);

			const buffers: Buffer[] = [];
			stream.on('data', (chunk: Buffer) => buffers.push(chunk));
			stream.once('end', () => resolve(Buffer.concat(buffers)));
			stream.once('error', (err: any) => reject(err));
		});
	}

	/** [throws] receive the payload of given max length as a single complete decoded string
	 *	automatically responds with given exceptions if the payload cannot be received properly */
	public async receiveAllText(encoding: string, maxLength: number | null): Promise<string> {
		/* wait for the buffer (let all errors propagate out) */
		const buffer: Buffer = await this.receiveAllBuffer(maxLength);

		try {
			return buffer.toString(encoding as BufferEncoding);
		} catch (err: any) {
			this.respondBadRequest({ reason: 'Unable to decode content' });
			throw err;
		}
	}

	/** drain any remaining data being uploaded (must not be called while a request is currently being received) */
	public async drainUpload(): Promise<void> {
		if (this.dropped)
			return;

		/* check if the data are currently being received and otherwise mark them as being received */
		if (this._state.receive == ReceiveState.receiving)
			return this.badClientUsage('Draining received data', false);
		this._state.receive = ReceiveState.receiving;

		/* check if any data remain in the queue to be received and drain them */
		if (this.hasDataQueued())
			await this.drainQueuedData();
		this._state.receive = ReceiveState.completed;
	}

	/** marks the object as having been handled and returns a web socket or automatically responds
	 *	with a corresponding error and returns null (automatically validates method and headers) */
	public async acceptWebSocket(): Promise<ClientSocket | null> {
		if (this.dropped || (this._state.response != ResponseState.none && this._state.response != ResponseState.preparing)) {
			this.trace('Request broken or already claimed, discarding websocket upgrade');
			return null;
		}

		/* check if the connection is a valid upgrade request (and ensure that the underlying web-server, also detected it) */
		let connection = libHelper.splitAndTrimList(this.headers.connection?.toLowerCase() ?? null, ',', false);
		if (connection.indexOf('upgrade') == -1 || this.headers?.upgrade?.toLowerCase() != 'websocket' || this.method != 'GET') {
			this.respondUpdateRequired('websocket');
			return null;
		}
		if (this._native.socket == null) {
			this.respondInternalError('Request was not provided as upgradable');
			return null;
		}
		const native = this._native.socket;

		/* mark the connection as being accepted (considered the header already having been sent) */
		this._state.response = ResponseState.headerSent;
		this._state.respondedResolve();
		this._state.upgrade = UpgradeState.upgrading;
		this.trace(`Performing upgrade on web socket connection: [${this.url.pathname}]`);

		/* await the actual websocket upgrade */
		const ws = await new Promise<ClientSocket | null>((resolve) => {
			let settled = false;

			/* register the broken listener and abort listeners (to detect failures of the upgrading or network errors) */
			const handleFailure = () => {
				if (settled) return; settled = true;
				this.error('Failed to upgrade to WebSocket');
				resolve(null);
			};
			this._state.droppedPromise.then(() => handleFailure());
			this._state.closedPromise.then(() => handleFailure());

			/* start the upgrade process (web-socket upgrade handler will automatically send error messages) */
			native.wss.handleUpgrade(this._request, native.socket, native.head, (ws, _) => {
				if (!settled && !this.dropped && this._state.response == ResponseState.headerSent) {
					settled = true, this._state.response = ResponseState.completed;

					/* ensure that the socket is valid as otherwise proper cleanup might not be guaranteed (no
					*	need to log errors, as this will trigger the broken state, which will be logged) */
					if (native.socket.destroyed)
						return resolve(null);

					/* clear the socket timeout (should already have been done in the first place by the web-socket-server) */
					this._request.socket.setTimeout(0);
					return resolve(ClientSocket._fromRequest(ws, this));
				}
				settled = true;
				this.dropConnection(ConnectionState.broken, 'Broken connection upgraded', false, false);
				resolve(null);
			});
		});
		this._state.upgrade = UpgradeState.upgraded;
		return ws;
	}
}

/**
 *	WebSocket with integrated alive checks.
 *	Structured WebSocket, which takes care of error handling.
 *	The 'close' event is guaranteed to fire exactly once and no 'data' events will follow.
 *	Takes ownership of the socket.
 */
export class ClientSocket extends ClientBase {
	private _ws: libWs.WebSocket;
	private _alive: {
		timer: null | NodeJS.Timeout;
		isAlive: boolean;
	};
	private _closing: {
		promise: Promise<void> | null;
		closed: (() => void) | null, defer: number;
	};
	private _emitter: libEvents.EventEmitter;
	private _log: {
		self: string;
		root: string;
		tagList: { value: string }[];
	}

	private constructor(ws: libWs.WebSocket, source: ClientRequest) {
		super(source, 'socket', source.server, source.config);
		this._ws = ws;
		this._alive = { timer: null, isAlive: true };
		this._closing = { promise: null, closed: null, defer: 0 };
		this._emitter = new libEvents.EventEmitter();

		this._ws.on('pong', () => {
			this.trace(`Alive check pong received`, { identity: this._log.self });
			this.selfIsAlive();
		});
		this._ws.on('message', (data) => {
			this.selfIsAlive();
			if (this._closing.promise != null || this._emitter.listenerCount('data') == 0)
				return;

			++this._closing.defer;
			const buffer: Buffer = (Buffer.isBuffer(data) ? data : (Array.isArray(data) ? Buffer.concat(data) : Buffer.from(data)));
			this.emitEventSync('data', buffer);
			--this._closing.defer;

			if (this._closing.promise != null)
				this.handleClosing();
		});
		this._ws.once('close', () => {
			this.handleClosing();

			/* check if any timers remain (must be the grace-kill timer, can be stopped, as this point can
			*	only be reached with an active timer, if defer was somehow still > 0, in which case its
			*	nested leaving will trigger the proper cleanup, but the timer is not necessary anymore) */
			if (this._alive.timer != null)
				clearTimeout(this._alive.timer);
			this._alive.timer = null;
		});
		this._ws.once('error', (err: any) => this.handleClosing(`WebSocket error: ${err.message}`));

		/* perserve the root identity for internal logs and the log extension of the base for open logs */
		this._log = { self: this.identity, root: '', tagList: [] };
		source.log(libLog._logs.buildWebSocketAcceptedLog(this.identity));
		const extIndex = source.identity.indexOf('.');
		if (extIndex > 0)
			this.logSetIdentity(`${this.identity}${source.identity.substring(extIndex)}`);
		this._log.root = this.identity;

		/* start the first alive check (no need to consider the socket timeout, as it will have been cleared already) */
		this.selfIsAlive();
	}
	private checkIsAlive(): void {
		if (this._closing.promise != null)
			return;

		this._alive.timer = null;
		if (this.config.webSocketTimeout == 0)
			return;

		/* check if the connection is not alive anymore and should be killed */
		if (!this._alive.isAlive || this.config.webSocketAliveTimeout == 0)
			return this.handleClosing('Closing dead websocket');
		this._alive.isAlive = false;
		this._alive.timer = setTimeout(() => this.checkIsAlive(), this.config.webSocketAliveTimeout);

		/* try to ping the remote to check the liveliness */
		try {
			this.trace(`Sending ping to determine if connection is alive`, { identity: this._log.self });
			this._ws.ping();
		} catch (err: any) {
			this.handleClosing(`WebSocket error while pinging: ${err.message}`);
		}
	}
	private selfIsAlive(): void {
		this._alive.isAlive = true;
		if (this._closing.promise != null)
			return;

		if (this._alive.timer != null)
			clearTimeout(this._alive.timer);
		this._alive.timer = (this.config.webSocketTimeout == 0 ? null : setTimeout(() => this.checkIsAlive(), this.config.webSocketTimeout));
	}
	private handleClosing(terminate?: string): void {
		/* register the initial closing to mark a closing being imminent */
		if (this._closing.promise == null) {
			this._closing.promise = new Promise<void>((res) => this._closing.closed = res);

			/* kill the last timer (alive timer) */
			if (this._alive.timer != null)
				clearTimeout(this._alive.timer);
			this._alive.timer = null;

			/* check if a termination should be triggered and otherwise start the grace termination timer
			*	(the timer is cleared by the 'close' event handler; if it fires, the close handshake has
			*	not completed within the grace period and the connection is forcefully terminated) */
			if (terminate != null) {
				this.error(terminate, { identity: this._log.self });
				this._ws.terminate();
			}
			else {
				this._alive.timer = setTimeout(() => {
					this._alive.timer = null;
					this.warning('Terminating connection', { identity: this._log.self });
					this._ws.terminate();
				}, this.config.killGraceTimeout);
			}
		}

		if (this._closing.closed == null || this._closing.defer > 0)
			return;
		const closed = this._closing.closed;
		this._closing.closed = null;

		this.emitEventSync('close');
		this.trace('Socket connection closed', { identity: this._log.self });

		closed();
	}
	private updateLogIdentity(): void {
		let identity = this._log.root;

		for (const tag of this._log.tagList) {
			if (tag.value != '')
				identity += `.${tag.value}`;
		}

		this.logSetIdentity(identity);
	}

	public static _fromRequest(ws: libWs.WebSocket, source: ClientRequest) {
		return new ClientSocket(ws, source);
	}

	/** send data to the remote (ignored if connection is being closed) */
	public send(data: string | Buffer): void {
		if (this._closing.promise != null)
			return;

		try {
			this._ws.send(data);
		} catch (err: any) {
			this.handleClosing(`WebSocket error while sending data: ${err.message}`);
		}
	}

	/** close the web socket (promise resolved once the close callback has been fully invoked;
	 *	must not be awaited within a message handler, as this can lead to deadlocks) */
	public close(): Promise<void> {
		if (this._closing.promise == null) {
			this._ws.close();
			this.handleClosing();
		}
		return this._closing.promise!;
	}

	/** tag the logging with the given identifier and return a callback to update the tag */
	public tagLog(identifier: string): SocketLogTag {
		let tag: { value: string } | null = { value: identifier };

		this._log.tagList.push(tag);
		if (tag.value != '')
			this.updateLogIdentity();

		/* setup the handler responsible to update the logging */
		return (value?: string) => {
			if (tag == null) return;

			/* check if the tag should be removed or if the value should just be updated */
			if (value == null) {
				this._log.tagList = this._log.tagList.filter((v) => v != tag);
				tag = null;
			}
			else if (value != tag.value)
				tag.value = value;

			this.updateLogIdentity();
		};
	}

	/* -------- event handler interfaces -------- */
	public on<K extends keyof ClientSocketEvents>(event: K, listener: ClientSocketEvents[K]): ClientSocket {
		this._emitter.on(event, listener); return this;
	}
	public off<K extends keyof ClientSocketEvents>(event: K, listener: ClientSocketEvents[K]): ClientSocket {
		this._emitter.off(event, listener); return this;
	}
	public once<K extends keyof ClientSocketEvents>(event: K, listener: ClientSocketEvents[K]): ClientSocket {
		this._emitter.once(event, listener); return this;
	}
	private emitEventSync<K extends keyof ClientSocketEvents>(event: K, ...args: Parameters<ClientSocketEvents[K]>): void {
		try {
			this._emitter.emit(event, ...args);
		}
		catch (err: any) {
			this.error(`Unhandled exception in ${event} listener: ${err.message}`, { identity: this._log.self });
		}
	}
}

export interface ClientConfig {
	/** default server name to be used in the http:server header [empty value prevents server header; Default: 'Modular Web Server'] */
	serverName?: string;

	/** default header values to be added to every http response [Default: { 'X-Content-Type-Options': 'nosniff' }] */
	commonHeaders?: Record<string, string>;

	/** default web-socket timeout before performing a ping to determine liveness [0 disables the timeout; in milliseconds; Default: 180_000] */
	webSocketTimeout?: number;

	/** default web-socket timeout to respond to a liveness ping before closing the connection [0 kills the connection without ping test; in milliseconds; Default: 2_000] */
	webSocketAliveTimeout?: number;

	/** time for a broken connection or socket to receive the response before force-closing it [0 results in immediate close; in milliseconds; Default: 1_000] */
	killGraceTimeout?: number;

	/** drain any uploaded data remaining in the pipeline, instead of closing the connection, but will even run and remain active on error responses [Default: false] */
	drainUpload?: boolean;

	/** cache-control values per cache policy */
	cache?: {
		/** default cache-control value for immutable content reads [empty string does not set any cache-control; Default: 'public, max-age=2592000, immutable' (30 days)] */
		immutable?: string;

		/** default cache-control value for normal content reads [empty string does not set any cache-control; Default: 'public, no-cache'] */
		static?: string;

		/** default cache-control value for any basic responses [empty string does not set any cache-control; Default: 'private, no-cache'] */
		private?: string;

		/** default cache-control value for any sensitive responses [empty string does not set any cache-control; Default: 'no-cache, no-store, max-age=0, must-revalidate'] */
		sensitive?: string;

		/** default cache-control value for no cache policy [empty string does not set any cache-control; Default: ''] */
		none?: string;
	};

	/** grace period before the throughput is started to be measured or for busy connections [in milliseconds; Default: 10_000] */
	throughputGrace?: number;

	/** throughput required for combined sending and receiving bodies of requests [0 disables the throughput check, in bytes/second; Default: 1_000] */
	throughputThreshold?: number;

	/** length of sliding time window for which the throughput must be above the threshold [in milliseconds; Default: 30_000] */
	throughputWindow?: number;
}

export class BurntClientConfig {
	public readonly serverName: string;
	public readonly commonHeaders: Record<string, string>;
	public readonly webSocketTimeout: number;
	public readonly webSocketAliveTimeout: number;
	public readonly killGraceTimeout: number;
	public readonly drainUpload: boolean;
	public readonly cache: Record<CachePolicy, string>;
	public readonly throughputGrace: number;
	public readonly throughputThreshold: number;
	public readonly throughputWindow: number;

	public constructor(config?: ClientConfig) {
		this.serverName = config?.serverName ?? 'Modular Web Server';
		this.commonHeaders = config?.commonHeaders ?? { 'X-Content-Type-Options': 'nosniff' };
		this.webSocketTimeout = config?.webSocketTimeout ?? 180_000;
		this.webSocketAliveTimeout = config?.webSocketAliveTimeout ?? 2_000;
		this.killGraceTimeout = config?.killGraceTimeout ?? 1_000;
		this.drainUpload = config?.drainUpload ?? false;
		this.cache = {
			immutable: config?.cache?.immutable ?? 'public, max-age=2592000, immutable',
			static: config?.cache?.static ?? 'public, no-cache',
			private: config?.cache?.private ?? 'private, no-cache',
			sensitive: config?.cache?.sensitive ?? 'no-cache, no-store, max-age=0, must-revalidate',
			none: config?.cache?.none ?? ''
		};
		this.throughputGrace = config?.throughputGrace ?? 10_000;
		this.throughputThreshold = config?.throughputThreshold ?? 1_000;
		this.throughputWindow = config?.throughputWindow ?? 30_000;
	}

	public static from(config?: ClientConfig | BurntClientConfig): BurntClientConfig {
		return (config instanceof BurntClientConfig ? config : new BurntClientConfig(config));
	}
}
