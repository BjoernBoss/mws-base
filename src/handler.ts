/* SPDX-License-Identifier: BSD-3-Clause */
/* Copyright (c) 2025-2026 Bjoern Boss Henrichsen */
import * as libClient from "./client.js";
import * as libLog from "./log.js";
import * as libBase from "./base.js";
import * as libServer from "./server.js";
import * as libCache from "./cache.js";
import * as libHelper from "./helper.js";

interface LinkedModules {
	parent: ModuleHandler | null;
	child: ModuleHandler;
	cleanup: () => Promise<void>;
	setup: ((realized: Promise<void> | null) => void) | null;
}

/**
*	Translation to be applied for nested children and reversed for any paths produced by the children.
*	Paths are matched by longest path. Unmapped paths will not be forwarded.
*	A null translation is considered not being mapped and will not be forwarded.
*	Reversed paths will implicitly use the identity translation for unmapped paths.
*	Mappings must be URI encoded, if their representation requires it, and will be
*	normalized to the canonical encoding (mappings with malformed encodings are skipped).
*/
export type PathTranslation = Record<string, string | null>;

/** Request and handler specific parameters to be passed to the corresponding handler */
export type Params = Record<string, any>;

export interface AttachedModule {
	/** forward the given client to the module, and translate the paths and logging accordingly; if the module
	 *	cannot process the request (stopping, translation does not apply, not attached), the call is a no-op;
	 *	params are passed on to the module without modification; no translation is equivalent to [/] => [/] */
	handle(client: libClient.ClientRequest, options?: { params?: Params, translate?: PathTranslation }): Promise<void>;

	/** detach the module from the parent module handler (registered unlinked callback will be invoked before this promise
	 *	is completed; clients dispatched to the module will not be disconnected directly and may still be active; if the
	 *	unlink results in the module being stopped, the promise first resolves once the module has fully stopped, and must
	 *	therefore not be awaited from within the stop handling of the attached module itself, as this can lead to deadlocks) */
	unlink(): Promise<void>;

	/** original module that was attached */
	module: ModuleHandler;

	/** check if the link is still valid (i.e. no unlink has been initiated) */
	linked(): boolean;

	/** resolves once the module has been unlinked (if the unlink results in the module being stopped, the promise
	 *	first resolves once the module has fully stopped, and must therefore not be awaited from within the stop
	 *	handling of the attached module itself, as this can lead to deadlocks) */
	unlinked(): Promise<void>;
}

/**
*	A module owns its entire subtree: once handleRequest is invoked, the request is guaranteed to be claimed
*	before the handler returns - if neither receiving nor responding was started, the framework defaults to
*	not-found. Parent modules can intercept any child response (including the auto not-found) via the patch
*	interface.
*	Modules will be stopped by default, once it is fully unlinked from the server again.
*	If a module is initialized, or stopped, the stop-handler is called.
*	An initialization will always be followed by a stop-handler call.
*	Any request will first happen after successful initialization, and will complete before stop-handlers.
*	WebSockets will not be automatically closed and must be closed by the module.
*	Modules can either be attached to other modules (allowing for recursive detaches/cleanups) or to the server.
*/
export abstract class ModuleHandler extends libLog.Logger {
	private _stopped: Promise<void> | null;
	private _config: {
		name: string;
		tagClients: boolean;
		tagString: string;
		stopOnDetach: boolean;
	};
	private _handling: {
		active: Map<libClient.ClientRequest, Promise<void> | null>;
		promise: Promise<void> | null;
		resolver: () => void;
	};
	private _attachment: {
		links: Set<LinkedModules>;
		server: libServer.Server | null;
		attached: boolean;
		task: {
			promise: Promise<void> | null;
			order: Promise<void>[];
			count: number;
			resolver: () => void;
		};
	};

	protected constructor(name: string) {
		super(name);

		this._config = { name, tagClients: true, tagString: '', stopOnDetach: true };
		this._handling = { active: new Map<libClient.ClientRequest, Promise<void> | null>(), promise: null, resolver: () => { } };
		this._stopped = null;
		this._attachment = { links: new Set<LinkedModules>(), attached: false, server: null, task: { promise: null, order: [], count: 0, resolver: () => { } } };
	}

	private async _drainTaskQueue(connections: boolean, order: Promise<void> | null): Promise<void> {
		const tasks = this._attachment.task;

		/* wait for the tasks and connections to drain accordingly */
		while (true) {
			const promises = [];

			if (tasks.promise != null)
				promises.push(tasks.promise);
			if (connections && this._handling.promise != null)
				promises.push(this._handling.promise);
			const index = (order == null ? tasks.order.length - 1 : tasks.order.indexOf(order) - 1);
			if (index >= 0)
				promises.push(tasks.order[index]);

			if (promises.length == 0)
				return;
			await Promise.all(promises);
		}
	}
	private _pushHandleTask(promise: Promise<void>): void {
		const task = this._attachment.task;

		if (task.count++ == 0)
			task.promise = new Promise((res) => task.resolver = res);
		promise.then(() => {
			if (--task.count == 0) {
				task.promise = null;
				task.resolver();
			}
		});
	}

	private _checkIsModuleAttached(handler: ModuleHandler): boolean {
		for (const entry of handler._attachment.links) {
			if (entry.parent == handler)
				continue;
			if (entry.parent != null && !entry.parent._attachment.attached)
				continue;
			return true;
		}
		return false;
	}
	private async _processIncomingClient(parent: ModuleHandler | null, client: libClient.ClientRequest, params?: Params, translate?: PathTranslation): Promise<void> {
		/* ensure that any outstanding tasks have completed and then check if the client can be processed by this module */
		await this._drainTaskQueue(false, null);
		if (!this._attachment.attached || this._handling.active.has(client) || client.claimed || client.server != this._attachment.server)
			return;
		if (parent != null && parent._handling.active.get(client) != null)
			return;

		/* setup the new mapping and path translation and check if the translation can be applied */
		const logTag = (this._config.tagClients ? (this._config.tagString == '' ? this.identity : this._config.tagString) : '');
		const snapshot = client._pushContext(translate ?? null, logTag);
		if (snapshot == null)
			return;

		/* check if the cleanup handler needs to be setup and push the client in general to be handled right now */
		if (this._handling.active.size == 0)
			this._handling.promise = new Promise<void>((res) => this._handling.resolver = res);
		this._handling.active.set(client, null);

		/* register the promise to the parent to ensure the parent awaits this handler */
		let completed: (() => void) | null = null;
		if (parent != null)
			parent._handling.active.set(client, new Promise<void>((res) => completed = res));

		/* handle the client, and default respond for any errors */
		try {
			await this.handleRequest(client, params);
		} catch (err: any) {
			client.respondInternalError(`Uncaught exception: ${err.message}`);
		}

		/* await the handling of any children and clear the parent's await */
		while (this._handling.active.get(client) != null)
			await this._handling.active.get(client);
		if (parent != null) {
			parent._handling.active.set(client, null);
			completed!();
		}

		/* restore the context (before removing the client, as it will respond, and may otherwise re-enter
		*	the handler), clear the handling promise and remove the client from actively handled clients */
		client._popContext(snapshot);
		this._handling.active.delete(client);
		if (this._handling.active.size == 0) {
			this._handling.promise = null;
			this._handling.resolver();
		}
	}
	private async _performAttachSelf(server: libServer.Server): Promise<void> {
		const firstAttachment = (this._attachment.server == null);
		this._attachment.attached = true;
		this._attachment.server = server;

		/* push the next ordered promise, to ensure the attach is fully performed before the next detach */
		let taskResolver = () => { };
		const taskPromise = new Promise<void>((res) => taskResolver = res);
		this._attachment.task.order.push(taskPromise);

		/* trigger any attach calls (may push new independent tasks) */
		for (const link of this._attachment.links) {
			if (link.setup != null)
				link.setup(taskPromise);
		}

		/* drain the current task queue and convert the task back to the unordered next step */
		await this._drainTaskQueue(true, taskPromise);
		this._attachment.task.order.shift();
		this._pushHandleTask(taskPromise);

		/* notify the module itself about the initialization */
		if (firstAttachment) {
			try { await this.handleInitialize(this._attachment.server); }
			catch (err: any) {
				this.error(`Unhandled exception while initializing: ${err.message}`);
				this.stop();
			}
			this.info(`Handler initialized to server [${this._attachment.server.identity}]`);
		}

		taskResolver();
	}
	private async _performDetachSelf(): Promise<void> {
		if (!this._attachment.attached)
			return;
		this._attachment.attached = false;

		/* push the next ordered promise, to ensure the detach is fully performed before the next attach */
		let taskResolver = () => { };
		const taskPromise = new Promise<void>((res) => taskResolver = res);
		this._attachment.task.order.push(taskPromise);

		/* trigger the detach of all now detached children */
		for (const entry of this._attachment.links) {
			if (entry.parent == this && !this._checkIsModuleAttached(entry.child))
				this._pushHandleTask(entry.child._performDetachSelf());
		}

		/* check if the module should be stopped due to being detached */
		if (this._config.stopOnDetach)
			this.stop();

		/* wait for any current tasks and connections to complete and remove the ordered task */
		await this._drainTaskQueue(false, taskPromise);
		this._attachment.task.order.shift();
		taskResolver();

		/* check if the module was stopped and await the proper stopping */
		if (this._stopped != null)
			await this._stopped;
	}
	private _performAttachToParent(parent: ModuleHandler | libServer.Server, unlinked: () => void, detail: string): AttachedModule {
		const recSearchParents = (parent: ModuleHandler): boolean => {
			if (parent == this)
				return false;
			for (const entry of parent._attachment.links) {
				if (entry.child == parent && entry.parent != null && !recSearchParents(entry.parent))
					return false;
			}
			return true;
		};
		const validateLinkState = (parentServer: libServer.Server | null): boolean => {
			if (this._stopped != null) {
				this.warning(`Stopped module cannot be attached to [${parent.identity}]`);
				return false;
			}
			const thisServer = this._attachment.server;
			if (thisServer != null && parentServer != null && thisServer != parentServer) {
				this.warning(`Module attached to server [${thisServer.identity}] cannot be attached to server [${parentServer.identity}]`);
				return false;
			}

			if (parent instanceof libServer.Server)
				return true;

			if (parent._stopped != null) {
				this.warning(`Module cannot be attached to stopped module [${parent.identity}]`);
				return false;
			}
			if (!recSearchParents(parent)) {
				this.warning('Module cannot be directly or indirectly attach to itself');
				return false;
			}
			return true;
		};
		if (detail != '')
			detail = `: ${detail}`;

		/* setup the link object and its creation and cleanup methods */
		let unlinkResolver = () => { }, taskResolver = () => { };
		const unlinkPromise: Promise<void> = new Promise<void>((res) => unlinkResolver = res);
		const taskPromise: Promise<void> = new Promise<void>((res) => taskResolver = res);
		let logged: boolean = false, stopping = false;
		const link: LinkedModules = {
			parent: (parent instanceof libServer.Server ? null : parent),
			child: this,
			setup: async (realized: Promise<void> | null): Promise<void> => {
				if (stopping)
					return;
				const parentServer = (parent instanceof libServer.Server ? parent : parent._attachment.server);

				/* check if the attachment is possible in theory */
				if (!validateLinkState(parentServer))
					link.cleanup();

				/* check if the attachment is still certain (uncertainty can only happen once on the first time
				*	attaching, as the second execution is performed after the parent has just been attached) */
				else if (parentServer != null) {
					link.setup = null;
					if (!this._attachment.attached) {
						if (realized != null)
							this._pushHandleTask(realized);
						await this._performAttachSelf(parentServer);
					}

					logged = !stopping;
					if (logged)
						this.log(`Attached to [${parent.identity}]${detail}`);
				}
			},
			cleanup: async (): Promise<void> => {
				if (stopping) return unlinkPromise;
				stopping = true, link.setup = null;
				if (logged)
					this.log(`Detached from [${parent.identity}]${detail}`);

				/* remove the link from the parent and add its cleanup to its task list (cleanup calls are only linked as task to the parent,
				*	as the child does not care for them; this implies that the module's stop method itself does not await them either) */
				if (!(parent instanceof libServer.Server)) {
					parent._attachment.links.delete(link);
					parent._pushHandleTask(taskPromise);
				}

				/* remove the link from this handler and check if the object should be detached */
				this._attachment.links.delete(link);
				if (!this._checkIsModuleAttached(this))
					await this._performDetachSelf();

				/* start the actual unlink call */
				process.nextTick(async () => {
					try { unlinked(); }
					catch (err: any) {
						this.error(`Unhandled exception while unlinking: ${err.message}`);
					}
					taskResolver();

					/* check if the module was stopped, in which case the stop should be awaited before resolving the cleanup promise */
					if (this._stopped != null)
						await this._stopped;
					unlinkResolver();
				});
				return unlinkPromise;
			}
		};

		/* register the link between the parent and this handler */
		this._attachment.links.add(link);
		if (!(parent instanceof libServer.Server))
			parent._attachment.links.add(link);

		/* try to immediately perform the initial load and setup the attach module interface */
		link.setup!(null);
		return {
			handle: (client, options) => (stopping ? Promise.resolve() : this._processIncomingClient((parent instanceof libServer.Server ? null : parent), client, options?.params, options?.translate)),
			unlink: () => link.cleanup(),
			module: this,
			linked: () => !stopping,
			unlinked: () => unlinkPromise
		};
	}

	public _rootAttachToServer(server: libServer.Server, unlinked: () => void): AttachedModule {
		return this._performAttachToParent(server, unlinked, '');
	}

	/** module is attached directly or indirectly to the server (will be the first call being performed before any other calls; will only be called once) */
	protected async handleInitialize(server: libServer.Server): Promise<void> { }

	/** handle the client request (guaranteed to not have been claimed yet; receiving or responding claims the client, which must
	 *	then be fully completed - response completed, receive consumed - before this promise resolves, as leftover streams will be
	 *	aborted; if the handler returns without claiming the request, the framework defaults to not-found - parent modules can use
	 *	the patch interface to intercept this response; long-running handlers must check 'client.claimed' or await 'client.responded'
	 *	to allow timely server shutdown) */
	protected abstract handleRequest(client: libClient.ClientRequest, params?: Params): Promise<void>;

	/** module has been stopped (all clients are guaranteed to have left, but accepted WebSockets will be left intact and
	 *	must be closed manually by this module; will only be called once; module should cleanup any resources and timers) */
	protected async handleStop(): Promise<void> { }

	/** name of the module */
	public get name(): string {
		return this._config.name;
	}

	/** server the module has been attached to (null if not yet initialized) */
	public get server(): libServer.Server | null {
		return this._attachment.server;
	}

	/** [throws] cache host to be used by this module (throws if not yet initialized) */
	public get cache(): libCache.CacheHost {
		if (this._attachment.server == null)
			throw new Error('Not yet initialized');
		return this._attachment.server.cache;
	}

	/** enable or disable the module tagging the logging of clients [default: true] */
	public tagClients(tag: boolean): this {
		this._config.tagClients = tag;
		return this;
	}

	/** set the tagging string the module should use in the logging of clients with empty string being module log identity [default: ''] */
	public tagString(tag: string): this {
		this._config.tagString = tag;
		return this;
	}

	/** enable or disable the module being stopped once detached the next time from the server [default: true] */
	public stopOnDetach(stop: boolean): this {
		this._config.stopOnDetach = stop;
		return this;
	}

	/** link the child to this module (unlinked will be called once the child has been removed or is identified to not
	 *	be suited for this module; will not be called if this module is not attached or will not be attached anymore;
	 *	unless already destroyed; detail is an additional information to be logged upon linking and unlinking) */
	public linkModule(child: ModuleHandler, unlinked?: () => void, detail?: string): AttachedModule {
		return child._performAttachToParent(this, (unlinked == null ? () => { } : unlinked), detail ?? '');
	}

	/** close any connections to the module and stop the module itself (stopping must not
	 *	be awaited while stopping itself or a parent as this can result in deadlocks) */
	public stop(): Promise<void> {
		if (this._stopped != null)
			return this._stopped;

		/* setup the stopped-promise (before performing any operations, as the first operations might otherwise
		*	call stop again, without the stopped-promise value being set, thereby recursively stopping again) */
		let resolver = () => { };
		this._stopped = new Promise<void>((res) => resolver = res);

		(async () => {
			/* kill any connections and kill all links and drain the remaining task queue
			*	(will not await the unlinked calls of links where this element is the child) */
			this._performDetachSelf();
			for (const client of this._handling.active)
				client[0].killConnection('Module detached');
			for (const link of this._attachment.links)
				link.cleanup();
			await this._drainTaskQueue(true, null);

			try { await this.handleStop(); }
			catch (err: any) {
				this.error(`Unhandled exception while stopping: ${err.message}`);
			}
			this.info('Handler stopped');

			resolver();
		})();

		return this._stopped;
	}
}

/**
*	Simple module handler implementation, which dispatches requests to different children based on the request path (longest match).
*	Used paths must be URI encoded. Stops itself once all children have been unlinked. Own parameters are not forwarded.
*/
export function dispatch(map: Record<string, ModuleHandler>, options?: { name?: string }): DispatchModule {
	return new DispatchModule(map, options);
}
export class DispatchModule extends ModuleHandler {
	private mapping: Record<string, AttachedModule>;

	constructor(map: Record<string, ModuleHandler>, options?: { name?: string }) {
		super(options?.name ?? 'dispatch');
		this.mapping = {};

		for (const [_key, handler] of Object.entries(map)) {
			/* normalize the encoding of the key, as it is matched against the canonically encoded client path */
			const [key, valid] = libHelper.normalizeEncodedPath(_key);
			if (!valid) {
				this.warning(`Ignoring mapping [${_key}] with malformed URI encoding by [${handler.identity}]`);
				continue;
			}
			if (key in this.mapping) {
				this.warning(`Ignoring duplicate mapping [${key}] by [${handler.identity}]`);
				continue;
			}

			this.mapping[key] = this.linkModule(handler, () => {
				delete this.mapping[key];
				if (Object.keys(this.mapping).length == 0)
					this.stop();
			}, `Bound to path [${key}]`);
		}

		if (Object.keys(this.mapping).length == 0)
			this.stop();
	}

	protected override async handleRequest(client: libClient.ClientRequest): Promise<void> {
		let bestMatch: string | null = null;

		/* iterate over the mappings and look for the corresponding best handler */
		for (const path in this.mapping) {
			if (!client.isSubPathOf(path))
				continue;
			if (bestMatch == null || bestMatch.length < path.length)
				bestMatch = path;
		}

		if (bestMatch != null) {
			client.trace(`Client dispatched to handler [${this.mapping[bestMatch].module.identity}] for path [${bestMatch}]`);
			await this.mapping[bestMatch].handle(client, { translate: { [bestMatch]: '/' } });
		}
		else
			client.trace(`Request cannot be dispatched`);
	}
}

/**
*	Simple module handler implementation, which dispatches requests to different children based on the request hostname (longest match).
*	Stops itself once all children have been unlinked. Own parameters are not forwarded.
*/
export function host(map: Record<string, ModuleHandler>, options?: { name?: string }): HostModule {
	return new HostModule(map, options);
}
export class HostModule extends ModuleHandler {
	private mapping: Record<string, AttachedModule>;

	constructor(map: Record<string, ModuleHandler>, options?: { name?: string }) {
		super(options?.name ?? 'host');

		this.mapping = {};
		for (const [host, handler] of Object.entries(map)) {
			if (host in this.mapping) {
				this.warning(`Ignoring duplicate mapping of host [${host}] by [${handler.identity}]`);
				continue;
			}

			this.mapping[host] = this.linkModule(handler, () => {
				delete this.mapping[host];
				if (Object.keys(this.mapping).length == 0)
					this.stop();
			}, `Bound to host [${host}]`);
		}

		if (Object.keys(this.mapping).length == 0)
			this.stop();
	}

	private testSubHost(host: string, test: string): boolean {
		if (host.length > test.length)
			return false;
		if (host.length == test.length)
			return (host == test);
		if (!test.endsWith(host))
			return false;
		return (test[test.length - host.length - 1] == '.' || host.startsWith('.') || host == '');
	}

	protected override async handleRequest(client: libClient.ClientRequest): Promise<void> {
		let bestMatch: string | null = null;

		/* iterate over the mappings and look for the corresponding best handler */
		for (const host in this.mapping) {
			if (!this.testSubHost(host, client.url.hostname))
				continue;
			if (bestMatch == null || bestMatch.length < host.length)
				bestMatch = host;
		}

		if (bestMatch != null) {
			client.trace(`Client dispatched to handler [${this.mapping[bestMatch].module.identity}] for host [${bestMatch}]`);
			await this.mapping[bestMatch].handle(client);
		}
		else
			client.trace(`Request cannot be dispatched`);
	}
}

/**
*	Simple module interface implementation, which forwards any requests to a child handler
*	with optional parameter and translation binding. Stops itself once the child has been unlinked.
*	Own parameters are not forwarded.
*/
export function bind(handler: ModuleHandler, options?: { params?: Params, translate?: PathTranslation, name?: string }): BindModule {
	return new BindModule(handler, options);
}
export class BindModule extends ModuleHandler {
	private handler: AttachedModule;
	private params?: Params;
	private translate?: PathTranslation;

	constructor(handler: ModuleHandler, options?: { params?: Params, translate?: PathTranslation, name?: string }) {
		super(options?.name ?? 'bind');

		this.handler = this.linkModule(handler, () => this.stop());
		this.params = options?.params;
		if (options?.translate != null)
			this.translate = { ...options?.translate };
	}

	protected override async handleRequest(client: libClient.ClientRequest): Promise<void> {
		await this.handler.handle(client, { params: this.params, translate: this.translate });
	}
}

/**
*	Simple module interface implementation, which validates the connected host and port before forwarding the client.
*	Stops itself once the child has been unlinked. Own parameters are not forwarded.
*/
export function check(handler: ModuleHandler, host: string | string[], options?: { name?: string, port?: number }): CheckModule {
	return new CheckModule(handler, host, options);
}
export class CheckModule extends ModuleHandler {
	private handler: AttachedModule;
	private port?: number;
	private hosts: string[];

	constructor(handler: ModuleHandler, host: string | string[], options?: { name?: string, port?: number }) {
		super(options?.name ?? 'check');

		this.handler = this.linkModule(handler, () => this.stop());
		this.hosts = (typeof host == 'string' ? [host] : host);
		this.port = options?.port;
	}
	private respondBadEndpoint(client: libClient.ClientRequest): void {
		client.respond(`No resource found at [${client.url.host}]:[${client.url.pathname}]`, {
			status: libBase.Status.NotFound,
			media: libBase.Media.Text,
			headers: { 'Connection': 'close' }
		});
		client.killConnection('Invalid endpoint description');
	}

	protected override async handleRequest(client: libClient.ClientRequest): Promise<void> {
		let matches = false;

		/* validate that the host matches */
		for (const host of this.hosts) {
			if (host == client.url.hostname) {
				matches = true;
				break;
			}
		}
		if (!matches) {
			client.warning(`Hostname [${client.url.hostname}] not allowed for this endpoint`);
			return this.respondBadEndpoint(client);
		}

		/* validate that the port matches */
		if (this.port != null && client.url.port) {
			if (parseInt(client.url.port, 10) != this.port) {
				client.warning(`Host [${client.url.port}] port does not match [${this.port}]`);
				return this.respondBadEndpoint(client);
			}
		}

		await this.handler.handle(client);
	}
}

/**
*	Simple module handler implementation, which allows requests to be handled by lambdas, and child modules to be attached.
*	Stops itself once all children have been unlinked. Forwards parameter to lambda functions.
*/
export function lambda(options?: { attach?: Record<string, ModuleHandler>, setup?: CallbackSetup, handle?: CallbackHandle, stop?: CallbackStop, name?: string }): LambdaModule {
	return new LambdaModule(options);
}
export class LambdaModule extends ModuleHandler {
	private setupLambda?: CallbackSetup;
	private handleLambda?: CallbackHandle;
	private stopLambda?: CallbackStop;
	private links: Record<string, AttachedModule>;
	private active: number;

	constructor(options?: { attach?: Record<string, ModuleHandler>, setup?: CallbackSetup, handle?: CallbackHandle, stop?: CallbackStop, name?: string }) {
		super(options?.name ?? 'lambda');
		this.setupLambda = options?.setup;
		this.handleLambda = options?.handle;
		this.stopLambda = options?.stop;

		this.links = {};
		this.active = 0;
		if (options?.attach != null) for (const name in options.attach) {
			++this.active;
			this.links[name] = this.linkModule(options.attach[name], () => {
				if (--this.active == 0)
					this.stop();
			});
		}

		if (options?.attach != null && this.active == 0)
			this.stop();
	}

	protected override async handleInitialize(server: libServer.Server): Promise<void> {
		if (this.setupLambda != null)
			await this.setupLambda(server, this.links);
	}
	protected override async handleRequest(client: libClient.ClientRequest, params?: Params): Promise<void> {
		if (this.handleLambda != null)
			await this.handleLambda(client, params, this.links);
	}
	protected override async handleStop(): Promise<void> {
		if (this.stopLambda != null)
			await this.stopLambda(this.links);
	}
}
export type CallbackSetup = (this: ModuleHandler, server: libServer.Server, links: Record<string, AttachedModule>) => Promise<void>;
export type CallbackHandle = (this: ModuleHandler, client: libClient.ClientRequest, params: Params | undefined, links: Record<string, AttachedModule>) => Promise<void>;
export type CallbackStop = (this: ModuleHandler, links: Record<string, AttachedModule>) => Promise<void>;
