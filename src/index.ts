/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `npm run dev` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `npm run deploy` to publish your worker
 *
 * Bind resources to your worker in `wrangler.toml`. After adding bindings, a type definition for the
 * `Env` object can be regenerated with `npm run cf-typegen`.
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

import { createClient, type ResponseJSON } from '@clickhouse/client-web';

interface Env {
	URL: string;
	USERNAME: string;
	PASSWORD: string;
}
export default {
	async fetch(request, env, ctx): Promise<Response> {

		const client = createClient({
			url: env.URL, // Replace with your ClickHouse endpoint
			username: env.USERNAME,
			password: env.PASSWORD,
			clickhouse_settings: {
				async_insert: 1,
				wait_for_async_insert: 1,
				async_insert_max_data_size: '1000000',
				async_insert_busy_timeout_ms: 1000,
			},
		});

		try {

			const url = new URL(request.url);

			if (url.pathname == '/ping') {
				const pingResult = await client.ping();
				console.log(pingResult);
				return new Response(JSON.stringify(pingResult));
			}

			if (url.pathname == '/query') {

				// Parse the 'limit' from the query string, default to 1000
				const limit = parseInt(url.searchParams.get('limit') || '1000', 10);

				// Validate 'imit' to prevent SQL injection or overly large queries
				const sanitizedLimit = isNaN(limit) || limit <= 0 ? 1000 : Math.min(limit, 10000);


				// Run the SELECT query with the sanitized limit
				const rows = await client.query({
					query: `SELECT * FROM default.request LIMIT ${sanitizedLimit}`,
					format: 'JSON',
				});

				// Parse the result as JSON
				const result = await rows.json<ResponseJSON<Record<string, any>>[]>();

				return new Response(JSON.stringify(result),
					{
						headers: { 'Content-Type': 'application/json' },
					});
			}



			const record = {
				asn: request.cf?.asn || null,
				asOrganization: request.cf?.asOrganization || null,
				colo: request.cf?.colo || null,
				httpProtocol: request.cf?.httpProtocol || null,
				tlsVersion: request.cf?.tlsVersion || null,
				tlsCipher: request.cf?.tlsCipher || null,
				requestPriority: request.cf?.requestPriority || null,
				edgeRequestKeepAliveStatus: request.cf?.edgeRequestKeepAliveStatus || null,
				country: request.cf?.country || null,
				isEUCountry: request.cf?.isEUCountry || null,
				continent: request.cf?.continent || null,
				city: request.cf?.city || null,
				postalCode: request.cf?.postalCode || null,
				latitude: request.cf?.latitude || null,
				longitude: request.cf?.longitude || null,
				timezone: request.cf?.timezone || null,
				region: request.cf?.region || null,
				regionCode: request.cf?.regionCode || null,
				metroCode: request.cf?.metroCode || null,
				timestamp: Math.floor(Date.now() / 1000),
				hostMetadata: request.cf?.hostMetadata || null,
				clientAcceptEncoding: request.cf?.clientAcceptEncoding || null,
				requestUrl: request?.url || null,
				requestMethod: request?.method || null,
				requestRedirect: request?.redirect || null,
				bodyUsed: request?.bodyUsed || null,
				accept: request.headers.get('accept') || null,
				acceptEncoding: request.headers.get('accept-encoding') || null,
				acceptLanguage: request.headers.get('accept-language') || null,
				cacheControl: request.headers.get('cache-control') || null,
				cfConnectingIp: request.headers.get('cf-connecting-ip') || null,
				cfIpCountry: request.headers.get('cf-ipcountry') || null,
				cfRay: request.headers.get('cf-ray') || null,
				cfVisitor: request.headers.get('cf-visitor') || null,
				connection: request.headers.get('connection') || null,
				host: request.headers.get('host') || null,
				pragma: request.headers.get('pragma') || null,
				priority: request.headers.get('priority') || null,
				secFetchDest: request.headers.get('sec-fetch-dest') || null,
				secFetchMode: request.headers.get('sec-fetch-mode') || null,
				secFetchSite: request.headers.get('sec-fetch-site') || null,
				secFetchUser: request.headers.get('sec-fetch-user') || null,
				upgradeInsecureRequests: request.headers.get('upgrade-insecure-requests') || null,
				userAgent: request.headers.get('user-agent') || null,
				xForwardedProto: request.headers.get('x-forwarded-proto') || null,
				xRealIp: request.headers.get('x-real-ip') || null,
			};

			const table = 'default.request';


			// Insert data asynchronously
			await client.insert({
				table,
				values: [record],
				format: 'JSONEachRow',
			});

			return new Response('Ok');

		} catch (error) {
			return new Response(`Error: ${error}`, { status: 500 });
		}
	},
} satisfies ExportedHandler<Env>;
