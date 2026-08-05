/* SPDX-License-Identifier: BSD-3-Clause */
/* Copyright (c) 2026 Bjoern Boss Henrichsen */
import * as libZlib from "zlib";
import * as libStream from "stream";

export interface StatusType {
	code: number;
	msg: string;
}
export const Status = {
	Ok: { code: 200, msg: 'OK' },
	Created: { code: 201, msg: 'Created' },
	PartialContent: { code: 206, msg: 'Partial Content' },
	SeeOther: { code: 303, msg: 'See Other' },
	NotModified: { code: 304, msg: 'Not Modified' },
	TemporaryRedirect: { code: 307, msg: 'Temporary Redirect' },
	PermanentRedirect: { code: 308, msg: 'Permanent Redirect' },
	BadRequest: { code: 400, msg: 'Bad Request' },
	Forbidden: { code: 403, msg: 'Forbidden' },
	NotFound: { code: 404, msg: 'Not Found' },
	MethodNotAllowed: { code: 405, msg: 'Method Not Allowed' },
	RequestTimeout: { code: 408, msg: 'Request Timeout' },
	Conflict: { code: 409, msg: 'Conflict' },
	PreconditionFailed: { code: 412, msg: 'Precondition Failed' },
	ContentTooLarge: { code: 413, msg: 'Content Too Large' },
	UnsupportedMediaType: { code: 415, msg: 'Unsupported Media Type' },
	RangeIssue: { code: 416, msg: 'Range Not Satisfiable' },
	UpgradeRequired: { code: 426, msg: 'Upgrade Required' },
	InternalError: { code: 500, msg: 'Internal Server Error' },
	HttpVersionNotSupported: { code: 505, msg: 'HTTP Version Not Supported' }
} as const satisfies Record<string, StatusType>

export interface MediaType {
	fileEnding: string[];
	mediaType: string;
	encoding: string;
	compressible: boolean;
}
export const Media = {
	Html: { fileEnding: ['html'], mediaType: 'text/html', encoding: 'charset=utf-8', compressible: true },
	Css: { fileEnding: ['css'], mediaType: 'text/css', encoding: 'charset=utf-8', compressible: true },
	JavaScript: { fileEnding: ['js', 'mjs', 'cjs'], mediaType: 'text/javascript', encoding: 'charset=utf-8', compressible: true },
	TypeScript: { fileEnding: ['ts', 'tsx', 'mts', 'cts'], mediaType: 'text/typescript', encoding: 'charset=utf-8', compressible: true },
	Text: { fileEnding: ['txt', 'text'], mediaType: 'text/plain', encoding: 'charset=utf-8', compressible: true },
	Json: { fileEnding: ['json'], mediaType: 'application/json', encoding: 'charset=utf-8', compressible: true },
	Mp4: { fileEnding: ['mp4'], mediaType: 'video/mp4', encoding: '', compressible: false },
	Png: { fileEnding: ['png'], mediaType: 'image/png', encoding: '', compressible: false },
	Gif: { fileEnding: ['gif'], mediaType: 'image/gif', encoding: '', compressible: false },
	Jpg: { fileEnding: ['jpg', 'jpeg'], mediaType: 'image/jpeg', encoding: '', compressible: false },
	Svg: { fileEnding: ['svg'], mediaType: 'image/svg+xml', encoding: 'charset=utf-8', compressible: true },
	Zip: { fileEnding: ['zip'], mediaType: 'application/zip', encoding: '', compressible: false },
	Archive7z: { fileEnding: ['7z'], mediaType: 'application/x-7z-compressed', encoding: '', compressible: false },
	Rar: { fileEnding: ['rar'], mediaType: 'application/vnd.rar', encoding: '', compressible: false },
	Tar: { fileEnding: ['tar'], mediaType: 'application/x-tar', encoding: '', compressible: false },
	Gzip: { fileEnding: ['gz'], mediaType: 'application/gzip', encoding: '', compressible: false },
	Unknown: { fileEnding: [], mediaType: 'application/octet-stream', encoding: '', compressible: false },
	TypeScriptMap: { fileEnding: ['ts.map'], mediaType: 'application/json', encoding: 'charset=utf-8', compressible: true },
	JavaScriptMap: { fileEnding: ['js.map'], mediaType: 'application/json', encoding: 'charset=utf-8', compressible: true }
} as const satisfies Record<string, MediaType>

export interface EncodingType {
	name: string;
	makeDecode(): libStream.Transform;
	makeEncode(): libStream.Transform;
	encodeBuffer(buffer: Buffer): Buffer;
}
export const Encoding = {
	Br: {
		name: 'br',
		makeDecode: () => libZlib.createBrotliDecompress(),
		makeEncode: () => libZlib.createBrotliCompress(),
		encodeBuffer: (buffer: Buffer): Buffer => libZlib.brotliCompressSync(buffer)
	},
	Zstd: {
		name: 'zstd',
		makeDecode: () => libZlib.createZstdDecompress(),
		makeEncode: () => libZlib.createZstdCompress(),
		encodeBuffer: (buffer: Buffer): Buffer => libZlib.zstdCompressSync(buffer)
	},
	Gzip: {
		name: 'gzip',
		makeDecode: () => libZlib.createGunzip(),
		makeEncode: () => libZlib.createGzip(),
		encodeBuffer: (buffer: Buffer): Buffer => libZlib.gzipSync(buffer)
	},
	Deflate: {
		name: 'deflate',
		makeDecode: () => libZlib.createInflate(),
		makeEncode: () => libZlib.createDeflate(),
		encodeBuffer: (buffer: Buffer): Buffer => libZlib.deflateSync(buffer)
	},
	Identity: {
		name: 'identity',
		makeDecode: () => new libStream.PassThrough(),
		makeEncode: () => new libStream.PassThrough(),
		encodeBuffer: (buffer: Buffer): Buffer => buffer
	}
} as const satisfies Record<string, EncodingType>

/** minimum size for content to be considered encodable */
export const MIN_ENCODING_SIZE: number = 1_000;
