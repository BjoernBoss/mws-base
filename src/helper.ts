/* SPDX-License-Identifier: BSD-3-Clause */
/* Copyright (c) 2026 Bjoern Boss Henrichsen */
import * as libBase from "./base.js";
import * as libLog from "./log.js";
import * as libUrl from "url";
import * as libPath from "path";

const helperLogger = libLog.createLoggerIdentity('helper');

/* setup the reverse list of file-endings to media types and encoding-names to encoding types */
const FileEndingToMediaTypeMapping: Record<string, libBase.MediaType> = {};
const EncodingNameToEncodingTypeMapping: Record<string, libBase.EncodingType> = {};
for (const media of Object.values(libBase.Media)) {
	for (const fileEnding of media.fileEnding)
		FileEndingToMediaTypeMapping[fileEnding] = media;
}
for (const encoding of Object.values(libBase.Encoding))
	EncodingNameToEncodingTypeMapping[encoding.name] = encoding;

/** lookup the encoding for a given name */
export function lookupEncoding(name: string): libBase.EncodingType | null {
	return EncodingNameToEncodingTypeMapping[name.toLowerCase()] ?? null;
}

/** list of all supported encodings */
export function supportedEncodingNames(): string[] {
	return Object.keys(EncodingNameToEncodingTypeMapping);
}

/** map extension of file-path/file-name to media type (null if no match was found) */
export function lookupMediaTypeFromFile(filePath: string): libBase.MediaType | null {
	let extension = '';

	/* loop, as file may have multiple extensions, such as ('.ab.cd') */
	while (true) {
		const [_, name, ext] = splitFileExtension(filePath);
		if (ext == '')
			return null;
		extension = ext + extension, filePath = name;

		const type = FileEndingToMediaTypeMapping[extension.substring(1).toLowerCase()] ?? null;
		if (type != null)
			return type;
	}
}

/** format the media type to the proper http header identifier */
export function buildMediaTypeIdentifier(media: libBase.MediaType): string {
	if (media.encoding == '')
		return media.mediaType;
	return `${media.mediaType}; ${media.encoding}`;
}

/** does not respect 'no-identity' encoding requests; unknown at-least-size is considered valid (defaults 'identity' to null) */
export function negotiateEncoding(accept: string | null, atLeastSize: number | null, media: libBase.MediaType): libBase.EncodingType | null {
	if (!media.compressible || accept == null)
		return null;
	if (atLeastSize != null && atLeastSize < libBase.MIN_ENCODING_SIZE)
		return null;

	/* parse the encoding types and their score */
	const scores: Record<string, number> = {};
	let bestScore: string | null = null;
	for (const part of splitAndTrimList(accept, ',', false)) {
		const segments = splitAndTrimList(part, ';', false);
		const name = segments[0].toLowerCase();

		/* check if the name is even supported and otherwise drop it */
		if (!(name in EncodingNameToEncodingTypeMapping) && name != '*')
			continue;

		/* parse the weight score of the value but default to 1.0 if none was given (ignore any with invalid quality) */
		let score = 1.0;
		for (let i = 1; i < segments.length; ++i) {
			const match = segments[i].match(/^\s*q\s*=\s*(\d+\.?\d*)\s*$/i);
			if (match != null) {
				score = parseFloat(match[1]);
				break;
			}
		}
		if (score < 0 || score > 1)
			continue;

		/* update the scores and best match */
		scores[name] = (name in scores ? Math.max(scores[name], score) : score);
		if (bestScore == null || scores[bestScore] < score)
			bestScore = name;
		else if (scores[bestScore] == score && (bestScore == name || bestScore == 'identity' || (bestScore == '*' && name != 'identity')))
			bestScore = name;
	}

	/* check if a best-match has been found */
	if (bestScore == null || scores[bestScore] <= 0)
		return null;
	if (bestScore != null && bestScore != '*')
		return (bestScore == 'identity' ? null : EncodingNameToEncodingTypeMapping[bestScore]);

	/* lookup the best entry not mentioned (because '*' was the best match; skip
	*	identity, as it translates to no encoding and does not need to be applied) */
	for (const encoding in EncodingNameToEncodingTypeMapping) {
		if (!(encoding in scores) && encoding != 'identity')
			return EncodingNameToEncodingTypeMapping[encoding];
	}
	return null;
}

/** ensure 'Accept-Encoding' is contained in the [Vary] header, as responses are subject to encoding negotiation (matches the header
 *	key and existing entries case-insensitively; a value of '*' is left untouched, as it already subsumes all request headers) */
export function extendVaryHeader(headers: Record<string, string>): void {
	const key = Object.keys(headers).find((k) => k.toLowerCase() == 'vary');
	if (key == null) {
		headers['Vary'] = 'Accept-Encoding';
		return;
	}

	/* check if the existing value already covers the encoding and otherwise extend it */
	const entries = splitAndTrimList(headers[key], ',', false).map((v) => v.toLowerCase()).filter((v) => v != '');
	if (!entries.includes('*') && !entries.includes('accept-encoding'))
		headers[key] = (entries.length == 0 ? 'Accept-Encoding' : `${headers[key]}, Accept-Encoding`);
}

export enum RangeState {
	noRange,
	valid,
	issue,
	malformed
}

/** parse an http header range request (first and last are correct for all valid range states; will be [0,-1] for
 *	an empty file; last positions past the file size and over-long suffix lengths are clamped to the file size) */
export function parseRangeHeader(range: string | null, fileSize: number): { first: number, last: number, state: RangeState } {
	if (range == null)
		return { first: 0, last: fileSize - 1, state: RangeState.noRange };

	/* ignore unknown range units (range units are case-insensitive) */
	if (range.length < 6 || range.substring(0, 6).toLowerCase() != 'bytes=')
		return { first: 0, last: fileSize - 1, state: RangeState.noRange };
	range = range.substring(6);

	/* extract the first number */
	let numberLength: number = 0, firstNumber: string = '', lastNumber: string = '';
	while (numberLength < range.length && (range[numberLength] >= '0' && range[numberLength] <= '9'))
		++numberLength;
	firstNumber = range.substring(0, numberLength);
	range = range.substring(numberLength);

	/* check if the separator exists */
	if (!range.startsWith('-'))
		return { first: 0, last: 0, state: RangeState.malformed };
	range = range.substring(1);

	/* extract the second number */
	numberLength = 0;
	while (numberLength < range.length && (range[numberLength] >= '0' && range[numberLength] <= '9'))
		++numberLength;
	lastNumber = range.substring(0, numberLength);
	range = range.substring(numberLength).trimStart();

	/* check if a valid end has been found or another range (only the first
	*	range will be respected) and that at least one number has been given */
	if (range != '' && !range.startsWith(','))
		return { first: 0, last: 0, state: RangeState.malformed };
	if (firstNumber == '' && lastNumber == '')
		return { first: 0, last: 0, state: RangeState.malformed };

	/* parse the two numbers */
	let first: number | null = (firstNumber.length == 0 ? null : parseInt(firstNumber));
	let last: number | null = (lastNumber.length == 0 ? null : parseInt(lastNumber));

	/* check if the range has an offset and potentially also an end */
	if (first != null) {
		/* an explicit last position before the first position is an invalid range specification */
		if (last != null && first > last)
			return { first: 0, last: 0, state: RangeState.malformed };

		/* clamp the last position to the file size and validate that the first position is satisfiable */
		if (first >= fileSize)
			return { first: 0, last: 0, state: RangeState.issue };
		if (last == null || last >= fileSize)
			last = fileSize - 1;
		return { first, last, state: RangeState.valid };
	}

	/* validate the suffix length and clamp it to the file size (an over-long suffix selects the entire file) */
	if (last! == 0 || fileSize == 0)
		return { first: 0, last: 0, state: RangeState.issue };
	if (last! > fileSize)
		last = fileSize;
	return { first: fileSize - last!, last: fileSize - 1, state: RangeState.valid };
}

/** check if the [etag] matches the list (i.e. in list or list is '*'), will not match for undefined list; if [strong]
*	comparison, both must be non-weak, opaque-tags equal (strip W/ prefix and compare opaque-tags regardless of weakness) */
export function etagMatchesList(etag: string, header: string | null, strong: boolean): boolean {
	if (header == null)
		return false;

	const list: string[] = splitAndTrimList(header, ',', true);
	if (list.length == 1 && list[0] == '*')
		return true;
	if (strong && etag.startsWith('W/'))
		return false;

	const target = etag.startsWith('W/') ? etag.substring(2) : etag;
	for (const entry of list) {
		const current = ((strong || !entry.startsWith('W/')) ? entry : entry.substring(2));
		if (target == current)
			return true;
	}
	return false;
}

/** returns null on invalid times, [>0] for a being greater, [<0] for a being smaller, [=0] for same time */
export function timestampCompare(a: string, b: string): number | null {
	const _a = new Date(a).getTime();
	if (isNaN(_a))
		return null;
	const _b = new Date(b).getTime();
	if (isNaN(_b))
		return null;
	return (_a - _b);
}

/** split a list value while removing whitespace and optionally respecting quotes (returns empty list on invalidly quoted strings) */
export function splitAndTrimList(content: string | null, separator: string, quotesAware: boolean): string[] {
	if (content == null)
		return [];

	let output: string[] = [], current = '', inQuote = false;
	for (const c of content) {
		if (c == '"' && quotesAware)
			inQuote = !inQuote, current += c;
		else if (c != separator || inQuote)
			current += c
		else
			output.push(current.trim()), current = '';
	}

	if (inQuote)
		return [];
	output.push(current.trim());

	return output;
}

/** escape all html-special characters to prevent injection when embedding untrusted values */
export function escapeHtml(content: string): string {
	let out = '';
	for (let i = 0; i < content.length; ++i) {
		switch (content[i]) {
			case '&': out += '&amp;'; break;
			case '<': out += '&lt;'; break;
			case '>': out += '&gt;'; break;
			case '"': out += '&quot;'; break;
			case '\'': out += '&#39;'; break;
			default: out += content[i]; break;
		}
	}
	return out;
}

/** expand the placeholders in the content (format: {#name}, with '{#' being escaped as '{##'; optionally html-escape values) */
export function expandPlaceholders(content: string, args: Record<string, string>, htmlEscape: boolean): string {
	let out = '', name = '', placeholder = false;
	for (let i = 0; i < content.length; ++i) {
		/* check if this is not the start/end of a placeholder, in which case it can just be added to the current set */
		if (!content.startsWith(placeholder ? '}' : '{#', i)) {
			if (placeholder)
				name += content[i];
			else
				out += content[i];
			continue;
		}

		/* check if a name is being started and if its potentially just an escape sequence */
		if (!placeholder) {
			if (content.startsWith('{##', i))
				out += '{#', i += 2;
			else
				name = '', placeholder = true, ++i;
			continue;
		}

		placeholder = false;
		if (name in args)
			out += (htmlEscape ? escapeHtml(args[name]) : args[name]);
		else
			helperLogger.warning(`Undefined placeholder [${name}] encountered`);
	}

	if (placeholder)
		helperLogger.warning('Content ends with an incomplete placeholder');
	return out;
}

/** escape all placeholders in the content */
export function escapePlaceholders(content: string): string {
	let out = '';

	/* construct the new escaped output content */
	for (let i = 0; i < content.length; ++i) {
		if (!content.startsWith('{#', i))
			out += content[i];
		else
			out += '{##', ++i;
	}
	return out;
}

/** normalize the URI encoding of the path to a canonical form by ensuring only the following characters are encoded
 *	using uppercase hex ('% / \ ? # [ ] < > ^ " ` { | }', space, control characters, and non-ascii), treating literal
 *	backslashes as path separators, and sanitize the final path (returns only the sanitized path and 'false', if the
 *	path contained malformed escape sequences or invalid UTF-8) */
export function normalizeEncodedPath(path: string): [string, boolean] {
	const NORMALIZED_VALID_CHARS = /%(24|26|2B|2C|3A|3B|3D|40)/g;
	const components = path.replaceAll('\\', '/').split('/');

	/* decode and re-encode every component to produce the canonical encoding */
	for (let i = 0; i < components.length; ++i) {
		/* try to decode the component and leave the entire path sanitized as-is on errors */
		let decoded = '';
		try { decoded = decodeURIComponent(components[i]); }
		catch (_) {
			return [sanitize(path, false), false];
		}

		/* re-encode the component to the reduced canonical set */
		components[i] = encodeURIComponent(decoded).replace(NORMALIZED_VALID_CHARS, (c) => decodeURIComponent(c));
	}

	/* create the final sanitized complete path */
	return [sanitize(components.join('/'), false), true];
}

/** sanitize path and remove relative path components and convert it to an absolute path;
 *	if [relative], path will be sanitized, but may remain relative, such as [../foo] */
export function sanitize(path: string, relative: boolean): string {
	/* treat the path as absolute, but preserve backward traversals into the root */
	let out = '/';
	if (path.startsWith('/'))
		relative = false;

	/* iterate over the characters and write them to the output
	*	(i == path.length is a final implicit slash to catch trailing '/..') */
	for (let i = 0; i <= path.length; ++i) {
		/* check if the character can just be written out */
		if (i < path.length && path[i] != '/' && path[i] != '\\') {
			out += path[i];
			continue;
		}

		/* check if the slash can be ignored as the string ends in a slash */
		if (out.endsWith('/'))
			continue;

		/* check if its a relative path step and remove it */
		if (out.endsWith('/.'))
			out = out.substring(0, out.length - 1);

		/* check if its just an arbitrary sequence */
		else if (!out.endsWith('/..')) {
			if (i + 1 >= path.length)
				break;
			out += '/';
		}

		/* process the backwards walking */
		else if (relative && (out.endsWith('/../..') || out == '/..')) {
			if (i < path.length)
				out += '/';
		}
		else if (!relative && out == '/..')
			out = '/';
		else
			out = out.substring(0, out.lastIndexOf('/', out.length - 4) + 1);
	}

	/* check if its not the root and remove trailing slashes and patch the relative path */
	if (out != '/') {
		if (out.endsWith('/'))
			out = out.substring(0, out.length - 1);
		if (relative)
			out = out.substring(1);
	}
	else if (relative)
		out = '.';
	return out;
}

/** join two paths into the sanitized absolute server path-environment */
export function joinSanitized(a: string, b: string): string {
	if (a.length == 0 || b.length == 0)
		return sanitize(a.length == 0 ? b : a, false);
	const aSlash = a.endsWith('/'), bSlash = b.startsWith('/');
	if (aSlash)
		return sanitize(bSlash ? a + b.substring(1) : a + b, false);
	return sanitize(bSlash ? a + b : `${a}/${b}`, false);
}

/** join two paths normalized as native paths */
export function joinNative(a: string, b: string): string {
	return libPath.join(a, b);
}

/** check if the sanitized path is a sub-path of or equal to the sanitized base path (can be /base or /base/...) */
export function isSubPath(base: string, path: string): boolean {
	if (base.length > path.length)
		return false;
	if (base.length == path.length)
		return (base == path);
	if (!path.startsWith(base))
		return false;
	return (path[base.length] == '/' || base.endsWith('/'));
}

/** check if the sanitized path is a true sub-path of the sanitized base path (must be truly inside: /base/...) */
export function isInside(base: string, path: string): boolean {
	if (base.length >= path.length)
		return false;
	if (!path.startsWith(base))
		return false;
	return ((path[base.length] == '/' && base.length + 1 < path.length) || base.endsWith('/'));
}

/** return the remaining path for the sub directory path in base (must be a true sub-directory) */
export function childPath(base: string, path: string): string {
	const out = path.substring(base.endsWith('/') ? base.length - 1 : base.length);
	return (out == '' ? '/' : out);
}

/** rebase the path from the old base directory onto the new base (must be a true sub-directory) */
export function rebasePath(oldBase: string, newBase: string, path: string): string {
	return joinSanitized(newBase, childPath(oldBase, path));
}

/** create path-creator, which returns sanitized paths inside of [path] (normalized to native path) */
export function createPathLocation(path: string): (path: string) => string {
	return function (p) {
		return joinNative(path, sanitize(p, false));
	};
}

/** create path-creator, which returns paths inside of the file url path (like the script itself
 *	using 'import.meta.url') and optionally changed by relative [path] (normalized to native path) */
export function createPathSelf(urlFilePath: string, path?: string): (path: string) => string {
	let dirName = libPath.dirname(libUrl.fileURLToPath(urlFilePath));
	if (path != null)
		dirName = joinNative(dirName, sanitize(path, true));
	return createPathLocation(dirName);
}

/** split the path in three components ['/base/', 'name', '.extension'] (extension will be empty if the path
*	does not contain a distinct extension; base will be empty if the path does not contain a distinct path) */
export function splitFileExtension(path: string): [string, string, string] {
	let dot: number | null = null;
	let name = path.length - 1;

	for (; name >= 0 && (path[name] != '/' && path[name] != '\\'); --name) {
		if (path[name] == '.' && dot == null)
			dot = name;
	}

	if (dot == null || dot == name + 1)
		dot = path.length;
	return [path.substring(0, name + 1), path.substring(name + 1, dot), path.substring(dot)];
}

/** split the path in two components ['/base/', 'name.extension'] (base will be empty if the path does not contain a distinct path) */
export function splitFileName(path: string): [string, string] {
	let name = path.length - 1;
	for (; name >= 0 && (path[name] != '/' && path[name] != '\\'); --name);
	return [path.substring(0, name + 1), path.substring(name + 1)];
}

/** trace log the configuration, and optionally the values, which differ from the reference */
export function logConfiguration(config: Record<string, any>, logger: libLog.Logger, options?: { identity?: string, prefix?: string, ref?: Record<string, any> }): void {
	const prefix = (options?.prefix ?? '');

	for (const [key, value] of Object.entries(config)) {
		const rValue = (options?.ref == undefined ? undefined : options.ref[key]);

		if (typeof value == 'object')
			logConfiguration(value, logger, { prefix: `${prefix}${key}.`, ref: rValue });
		else if (value == rValue)
			continue;
		else if (typeof value == 'string')
			logger.trace(`Config [${prefix}${key}]: '${value}'`, { identity: options?.identity });
		else
			logger.trace(`Config [${prefix}${key}]: ${value}`, { identity: options?.identity });
	}
}
