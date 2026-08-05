/* SPDX-License-Identifier: BSD-3-Clause */
/* Copyright (c) 2024-2026 Bjoern Boss Henrichsen */
import * as libFs from "fs";

const DEFAULT_FILE_FLUSHING_DELAY: number = 2_000;
const DEFAULT_FILE_BUF_MAXIMUM_LINES = 1_000;
const DEFAULT_FILE_SIZE_SWAP_FILE = 10_000_000;

const LoggerIdMap: Record<string, number> = {};
const GlobalLogConsumers: Set<LogConsumer> = new Set<LogConsumer>();

function formatLevel(level: LogLevel): string {
	switch (level) {
		case 'log':
			return 'Log  ';
		case 'trace':
			return 'Trace';
		case 'error':
			return 'Error';
		case 'info':
			return 'Info ';
		case 'warning':
			return 'Warn ';
	}
}
function formatLine(level: LogLevel, date: string, identity: string, msg: string, lineBreak: boolean): string {
	let printLevel: string = formatLevel(level);
	if (lineBreak)
		return `[${date}] ${printLevel}: [${identity}] ${msg}\n`;
	return `[${date}] ${printLevel}: [${identity}] ${msg}`;
}
function settleWrapper(consumer: LogConsumer): LogConsumer {
	let settled = false;
	return (level: LogLevel | null, date: string, identity: string, msg: string): void => {
		if (settled) return;
		if (level == null) settled = true;
		consumer(level, date, identity, msg);
	};
}

export const _logs = {
	REQUEST_CONNECT_REGEX: /^Connected to \[(.*)\] using \[method: (.*)\] from (\[.*\]:\d+) to \[(.*)\] \(user-agent: \[(.*)\]\)$/,
	buildConnectedLog: function (endpoint: string, method: string, address: string, port: number, url: string, userAgent: string): string {
		return `Connected to [${endpoint}] using [method: ${method}] from [${address}]:${port} to [${url}] (user-agent: [${userAgent}])`;
	},

	REQUEST_COMPLETED_REGEX: /^Completed on \[.*\]$/,
	buildCompletedLog: function (endpoint: string): string {
		return `Completed on [${endpoint}]`;
	},

	REQUEST_RESPONSE_REGEX: /^Responding with (\d+):\[([^\]]*)\](?: of size \[([^\]]*)\] and type \[([^\]]*)\]| with no content)(?:: (.*))?$/,
	buildResponseLog: function (description: string): string {
		return `Responding with ${description}`;
	},
	buildResponseDescription: function (code: number, msg: string, content: { size?: number, type: string } | null, detail: string | null): string {
		let output = `${code}:[${msg}]`;
		if (content == null)
			output += ' with no content';
		else
			output += ` of size [${content.size ?? 'unknown'}] and type [${content.type}]`;
		if (detail != null)
			output += `: ${detail}`;
		return output;
	},

	REQUEST_FILE_DETAIL_REGEX: /^(?:File \[[^\]]*\] from|Empty file) \[(.*?)\]( .*)?$/,
	buildFileEmpty: function (filePath: string): string {
		return `Empty file [${filePath}]`;
	},
	buildFileDetail: function (first: number, last: number, total: number, filePath: string): string {
		return `File [${first} - ${last}/${total}] from [${filePath}]`;
	},

	REQUEST_CONNECTION_STATUS_REGEX: /^Connection (broken|disconnected): \[(.*)\]$/,
	buildConnectionStatusLog: function (disconnected: boolean, reason: string): string {
		return `Connection ${disconnected ? 'disconnected' : 'broken'}: [${reason}]`;
	},

	REQUEST_WEBSOCKET_REGEX: /^WebSocket accepted: \[(.*)\]$/,
	buildWebSocketAcceptedLog: function (identity: string): string {
		return `WebSocket accepted: [${identity}]`;
	}
} as const;

/** supported log level */
export type LogLevel = 'error' | 'info' | 'warning' | 'log' | 'trace';

/** if [level] is null: is not a log, but the callback is being unregistered */
export type LogConsumer = (level: LogLevel | null, date: string, identity: string, msg: string) => void;

/** type to invoke to detach the given logger */
export type Detacher = () => void;

/** implementation of a console logger */
export function createConsoleLogger(): LogConsumer {
	return settleWrapper((level: LogLevel | null, date: string, identity: string, msg: string) => {
		if (level == null)
			return;
		let levelPrint: string = formatLevel(level);
		if (process.stdout?.hasColors == null || !process.stdout.hasColors())
			return console.log(formatLine(level, date, identity, msg, false));

		let levelColor = '';
		switch (level) {
			case 'log':
				levelColor = '\x1b[37m';
				break;
			case 'trace':
				levelColor = '\x1b[94m';
				break;
			case 'error':
				levelColor = '\x1b[31m';
				break;
			case 'info':
				levelColor = '\x1b[92m';
				break;
			case 'warning':
				levelColor = '\x1b[33m';
				break;
		}

		console.log(`\x1b[90m[${date}] ${levelColor}${levelPrint}\x1b[0m: [\x1b[93m${identity}\x1b[0m] ${msg}`);
	});
}

/** implementation of a logger which receives a well formatted line */
export function createLineLogger(cb: (line: string) => void): LogConsumer {
	return settleWrapper((level: LogLevel | null, date: string, identity: string, msg: string) => {
		if (level != null)
			cb(formatLine(level, date, identity, msg, false));
	});
}

/** implementation of a logger which receives the separate log components (cleanup of log consumer is automatically consumed and enforced) */
export function createLogger(log: (level: LogLevel, date: string, identity: string, msg: string) => void): LogConsumer {
	return settleWrapper((level: LogLevel | null, date: string, identity: string, msg: string) => {
		if (level != null) log(level, date, identity, msg);
	});
}

/** file overwriting preservation mode */
export enum PreserveMode {
	/** simply clear the log file once full */
	none,

	/** preserve the last log file to 'filePath.old' */
	last,

	/** preserve all last log files to 'filePath.%time%.old' */
	all
}

/** implementation of a file logger, which logs into the file-path and optionally preserves old logs
*	(Note: contains a timer, detach the logger via its detacher on shutdown to ensure fast shutdown) */
export function createFileLogger(filePath: string, options?: { flushingDelayMs?: number, bufMaxLineCount?: number, sizeSwapFile?: number, preserve?: PreserveMode }): LogConsumer {
	/* setup the logging state (ignore any errors, as they cannot be logged) */
	let fileHandle: number | null = null;
	let logFileSize: number = 0;
	try {
		fileHandle = libFs.openSync(filePath, 'a');
		logFileSize = libFs.fstatSync(fileHandle).size;
	}
	catch (_) { }

	/* setup the file-flushing helper function */
	let logBuffer: string[] = [];
	let flushId: NodeJS.Timeout | null = null;
	const flushToFile = () => {
		/* write the buffer to the file */
		if (logBuffer.length > 0 && fileHandle != null) {
			const content = Buffer.from(logBuffer.join(""), 'utf-8');
			try { libFs.writeFileSync(fileHandle, content); } catch (_) { }
			logFileSize += content.length;
		}
		logBuffer = [];

		/* clear any currently queued flushes */
		if (flushId != null) {
			clearTimeout(flushId);
			flushId = null;
		}
	};
	const flushingDelayMs: number = (options?.flushingDelayMs == null ? DEFAULT_FILE_FLUSHING_DELAY : options.flushingDelayMs);
	const bufMaxLineCount: number = (options?.bufMaxLineCount == null ? DEFAULT_FILE_BUF_MAXIMUM_LINES : options.bufMaxLineCount);
	const sizeSwapFile: number = (options?.sizeSwapFile == null ? DEFAULT_FILE_SIZE_SWAP_FILE : options.sizeSwapFile);

	/* setup the actual closure handler */
	return settleWrapper((level: LogLevel | null, date: string, identity: string, msg: string) => {
		/* check if the logger is being disabled and reset the file state */
		if (level == null) {
			flushToFile();
			if (fileHandle != null)
				try { libFs.closeSync(fileHandle); } catch (_) { }
			fileHandle = null;
			return;
		}

		/* write the log to the buffer and check if the data need to be flushed inplace, or if the flushing can be delayed */
		logBuffer.push(formatLine(level, date, identity, msg, true));
		if (logBuffer.length >= bufMaxLineCount)
			flushToFile();
		else {
			if (flushId != null)
				clearTimeout(flushId);
			flushId = setTimeout(flushToFile, flushingDelayMs);
		}

		/* check if the log-files need to be swapped */
		if (logFileSize < sizeSwapFile)
			return;

		/* flush the buffered entries and close the current file */
		flushToFile();
		if (fileHandle != null) {
			try { libFs.closeSync(fileHandle); } catch (_) { }
			fileHandle = null;
		}

		/* preserve the old file to be used or remove it */
		if (options?.preserve == PreserveMode.all)
			try { libFs.renameSync(filePath, `${filePath}.${Date.now()}.old`); } catch (_) { }
		else if (options?.preserve == PreserveMode.last)
			try { libFs.renameSync(filePath, `${filePath}.old`); } catch (_) { }
		else
			try { libFs.unlinkSync(filePath); } catch (_) { }

		try { fileHandle = libFs.openSync(filePath, 'w'); } catch (_) { }
		logFileSize = 0;
	});
}

/** implementation of a log filter, which only forwards logs, which the filter callback deem relevant, based on the level, identity, and message */
export function createLogFilter(target: LogConsumer, filter: (level: LogLevel, identity: string, msg: string) => boolean): LogConsumer {
	return settleWrapper((level: LogLevel | null, date: string, identity: string, msg: string) => {
		if (level == null)
			return target(null, '', '', '');

		/* check if the message should be logged */
		if (filter(level, identity, msg))
			target(level, date, identity, msg);
	});
}

/** implementation of a request logger, which condenses the various logs produced per request/upgrade into a single
*	line per relevant event (response sent, connection issue, websocket accepted) and formatted using the given patterns
*	(unknown placeholders are left as-is, missing values are set to '-'; unknown request behavior is logged as an issue).
*	The logger consumes request flow logs (connect > response/issue/socket > completed).
*	- all requests: %{ENDPOINT} (endpoint), %{METHOD} (method), %{REMOTE} (remote), %{URL} (url), %{AGENT} (user-agent)
*	- response: %{CODE}, %{STATUS}, %{SIZE}, %{TYPE}, %{DETAIL} (detail contains additional response data or the file sent)
*		=> Default: '%{METHOD} [%{URL}] => %{CODE} (%{STATUS}) [%{DETAIL}]'
*	- issue: %{WHAT}, %{REASON}
*		=> Default: '%{METHOD} [%{URL}] => %{WHAT}'
*	- socket: %{SOCKET}
*		=> Default: '%{METHOD} [%{URL}] => WebSocket Accepted (%{SOCKET})'
*	- request: %{MSG} (request related logs, which where not part of the parsed request flow)
*		=> Default: '%{METHOD} [%{URL}] => %{MSG}'
*	- other: %{MSG} (used for any logs not associated with a request or before/after the request cycle)
*		=> Default: '%{MSG}' */
export function createRequestLogger(target: LogConsumer, pattern?: { response?: string, issue?: string, socket?: string, request?: string, other?: string }): LogConsumer {
	const open: Record<string, { values: Record<string, string>, logged: boolean }> = {};

	const patternResponse = pattern?.response ?? '%{METHOD} [%{URL}] => %{CODE} (%{STATUS}) [%{DETAIL}]';
	const patternIssue = pattern?.issue ?? '%{METHOD} [%{URL}] => %{WHAT}';
	const patternSocket = pattern?.socket ?? '%{METHOD} [%{URL}] => WebSocket Accepted (%{SOCKET})';
	const patternRequest = pattern?.request ?? '%{METHOD} [%{URL}] => %{MSG}';

	const expandPattern = (pattern: string, index: string | null, params: Record<string, string>): string => {
		const entry = (index == null ? null : open[index]);
		if (entry != null)
			entry.logged = true;

		const values: Record<string, string> = { ...entry?.values, ...params };
		return pattern.replace(/%\{(\w+)\}/g, (match, key) => values[key] ?? match);
	};

	const handleLog = (level: LogLevel, date: string, identity: string, msg: string): boolean => {
		if (!identity.startsWith('request!') && !identity.startsWith('upgrade!'))
			return false;
		const endOfId = identity.indexOf('.');
		const index = (endOfId < 0 ? identity : identity.substring(0, endOfId));

		/* check if this is the initial connection */
		if (!(index in open)) {
			const match = msg.match(_logs.REQUEST_CONNECT_REGEX);
			if (match == null)
				return false;
			open[index] = { values: { ENDPOINT: match[1], METHOD: match[2], REMOTE: match[3], URL: match[4], AGENT: match[5] }, logged: false };
			return true;
		}

		/* check if the request has been completed */
		if (msg.match(_logs.REQUEST_COMPLETED_REGEX)) {
			if (!open[index].logged)
				target('error', date, identity, expandPattern(patternIssue, index, { WHAT: 'Logger issue', REASON: 'No behavior was found to be logged' }));
			delete open[index];
			return true;
		}

		/* check if the response has been sent (a file response rewrites the detail to the file path) */
		const response = msg.match(_logs.REQUEST_RESPONSE_REGEX);
		if (response != null) {
			const params: Record<string, string> = { CODE: response[1], STATUS: response[2], SIZE: response[3] ?? '-', TYPE: response[4] ?? '-' };
			const file = (response[5] == null ? null : response[5].match(_logs.REQUEST_FILE_DETAIL_REGEX));

			params.DETAIL = (file != null ? `File: ${file[1]}` : response[5] ?? '-');
			target(level, date, identity, expandPattern(patternResponse, index, params));
			return true;
		}

		/* check if a connection issue was encountered */
		const connection = msg.match(_logs.REQUEST_CONNECTION_STATUS_REGEX);
		if (connection != null) {
			target(level, date, identity, expandPattern(patternIssue, index, { WHAT: `Connection ${connection[1]}`, REASON: connection[2] }));
			return true;
		}

		/* check if a websocket was accepted */
		const websocket = msg.match(_logs.REQUEST_WEBSOCKET_REGEX);
		if (websocket != null) {
			target(level, date, identity, expandPattern(patternSocket, index, { SOCKET: websocket[1] }));
			return true;
		}

		/* expand the default request pattern */
		target(level, date, identity, expandPattern(patternRequest, index, { MSG: msg }));
		return true;
	};

	return settleWrapper((level: LogLevel | null, date: string, identity: string, msg: string): void => {
		if (level == null)
			return target(level, date, identity, msg);
		if (handleLog(level, date, identity, msg))
			return;

		if (pattern?.other != null)
			target(level, date, identity, expandPattern(pattern.other, null, { MSG: msg }));
		else
			target(level, date, identity, msg);
	});
}

/** create a standalone logger for the given identity */
export function createLoggerIdentity(identity: string): Logger {
	return new Logger(identity);
}

/** logger class to extend, supporting various logging levels, and writing to the registered log consumers */
export class Logger {
	private _logIdentity: string;

	public constructor(identity: string) {
		const id = (LoggerIdMap[identity] ?? 0) + 1;
		LoggerIdMap[identity] = id;

		this._logIdentity = `${identity}!${id}`;
	}
	private _performActualLog(level: LogLevel, msg: string, options?: { identity?: string }): void {
		const identity = (options?.identity == null ? this._logIdentity : options.identity);
		logGlobal(level, identity, msg);
	}

	/** update the log identity */
	protected logSetIdentity(identity: string): void {
		this._logIdentity = identity;
	}

	/** logging identity as shown in logs */
	public get identity(): string {
		return this._logIdentity;
	}

	public error(msg: string, options?: { identity?: string }): void {
		this._performActualLog('error', msg, options);
	}
	public info(msg: string, options?: { identity?: string }): void {
		this._performActualLog('info', msg, options);
	}
	public warning(msg: string, options?: { identity?: string }): void {
		this._performActualLog('warning', msg, options);
	}
	public log(msg: string, options?: { identity?: string }): void {
		this._performActualLog('log', msg, options);
	}
	public trace(msg: string, options?: { identity?: string }): void {
		this._performActualLog('trace', msg, options);
	}
}

/** register another global logger to receive the logs (returned detacher can be invoked to remove the log) */
export function addLogger(cb: LogConsumer): Detacher {
	let detached = false;
	const wrapped = (level: LogLevel | null, date: string, identity: string, msg: string): void => {
		if (detached) return;

		if (level == null) {
			GlobalLogConsumers.delete(wrapped);
			detached = true;
		}

		try {
			if (detached)
				cb(null, '', '', '');
			else
				cb(level, date, identity, msg);
		}
		catch (err: any) {
			console.error(`Logger failed: ${err.message}`);
			wrapped(null, '', '', '');
		}
	};
	GlobalLogConsumers.add(wrapped);

	return () => {
		wrapped(null, '', '', '');
	};
}

/** perform a global log to all registered loggers */
export function logGlobal(level: LogLevel, identity: string, msg: string): void {
	const date = new Date().toUTCString();
	for (const cb of GlobalLogConsumers)
		cb(level, date, identity, msg);
}
