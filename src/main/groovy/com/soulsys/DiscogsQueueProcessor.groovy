package com.soulsys

import groovyx.gpars.group.DefaultPGroup
import groovyx.net.http.ContentType
import groovyx.net.http.HTTPBuilder;
import groovyx.net.http.Method

import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.BlockingQueue

import com.mongodb.Response;
import com.sun.xml.internal.ws.api.streaming.XMLStreamReaderFactory.Default;

/**
 * Created with IntelliJ IDEA.
 * User: lcollins
 * Date: 12/23/13
 * Time: 3:19 AM
 * To change this template use File | Settings | File Templates.
 */

class RequestProcessor {

	def urlPrefix = "http://api.discogs.com";

	def getRelease(releaseId, cb) {
		def url = "/releases/$releaseId"
		getDiscogsData(Method.GET, url, null, cb)
	}

	def getArtistReleases(artistId, page, cb) {
		page = page ?: 1
		def url = "/artists/$artistId/releases"
		def q = [page: page]
		getDiscogsData(Method.GET, url, q, cb)
	}

	def getMasterRelease(masterId, cb) {
		def url = "/masters/$masterId"
		getDiscogsData(Method.GET, url, null, cb)
	}

	def getArtist(artistId, cb) {
		def url = "/artists/$artistId"
		getDiscogsData(Method.GET, url, null, cb)
	}

	def getVersions(masterId, cb, page) {
		page ?: 1
		def url = "/masters/$masterId/versions"
		getDiscogsData(Method.GET, url, [page: page], cb)
	}

	def getLabel(labelId, cb) {
		def url = "/labels/$labelId"
		getDiscogsData(Method.GET, url, [:], cb)
	}

	def getDiscogsData(method, url, query, cb) {
		if (discogsRequestProcessor.isActive()) {
			discogsRequestProcessor.send([
				method: method,
				url: url,
				query: query,
				cb: cb
			])
		}
	}

	BlockingQueue bq = new ArrayBlockingQueue(20000);

	def group = new DefaultPGroup()
	def stopped = false;
	final def discogsRequestProcessor = group.actor {
		loop {
			react { request ->

				if (request.stopIt) {
					stopped = true;
					return;
				}
				///request =  ( method, url, query, cb )

				//               println("discogsRequestProcessor: queuing $request")
				bq.put(request)

				/// create promise

				/// add it to the request

				/// check the request type

			}
		}
	}//.start()
	def http
	def start() {
		//        discogsRequestProcessor.start()

		http = new HTTPBuilder('http://api.discogs.com')
		http.handler.failure = { resp -> println "Unexpected failure: ${resp.statusLine}" }

		while (!stopped) {
			def req = bq.take();
			def s =bq.size()
			println("reqProcessor: dequed($s): $req")
			Thread.start{
				processRequest(req);
			}
			Thread.sleep(1001);
		}
	}

	def processRequest(request) {
		def url  = urlPrefix + request.url
		println "Processing req for [$request.url]"
		def bFull = request.url.toString().contains("http://")

		if(!bFull) {
			http.request( request.method, ContentType.JSON) {
				uri.path = request.url
				//TODO fornow uri.query = request.query
				response.success = { resp, json ->
					request.cb(json)
				}//resp
				// executed only if the response status code is 404:
				response.'404' = { resp ->
					println ("Resource not found: "+url)
				}

			}}//request
		else {
			http.request(request.url, request.method, ContentType.JSON) {

				//TODO fornow uri.query = request.query
				response.success = { resp, json ->
					request.cb(json)
				}//resp
				// executed only if the response status code is 404:
				response.'404' = { resp ->
					println ("Resource not found: "+url)
				}

			}//request
		}
	}

}