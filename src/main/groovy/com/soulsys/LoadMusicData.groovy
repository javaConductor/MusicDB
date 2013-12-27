package com.soulsys

import com.gmongo.GMongo
import com.sun.org.apache.bcel.internal.generic.LSTORE;
import com.sun.org.apache.bcel.internal.generic.RET;
import com.sun.xml.internal.ws.api.streaming.XMLStreamReaderFactory.Default

import org.bson.types.ObjectId;

import static groovyx.net.http.Method.GET;
//@Grab('org.codehaus.groovy.modules.http-builder:http-builder:0.5.2')
/**
 * Created with IntelliJ IDEA.
 * User: lcollins
 * Date: 12/22/13
 * Time: 12:21 AM
 * To change this template use File | Settings | File Templates.
 */
class LoadMusicData {
	static void main(String[] args) {
		def mongo = new GMongo()
		def db = mongo.getDB("MusicLibrary");
		new LoadMusicData(db, new com.soulsys.RequestProcessor())
	}//main

	def db
	def reqProcessor

	def LoadMusicData(mongodb, reqProc) {
		db = mongodb
		db.Releases.ensureIndex(["d_release_id":1 ], ["unique" : true, "dropDups" : true]);
		db.Artists.ensureIndex(["d_artist_id":1 ], ["unique":true,"dropDups" : true]);
		db.Labels.ensureIndex(["d_label_id":1 ], ["unique":true,"dropDups" : true])
		db.MasterReleases.ensureIndex(["d_master_release_id" : 1 ], ["unique":true,"dropDups" : true])
		reqProcessor = reqProc
		start();
	}

	def handleArtistReleases = { respObj ->
		def pagination = respObj.pagination;
		if (pagination?.urls?.next) {
			reqProcessor.getDiscogsData(GET, pagination.urls.next, [:], handleArtistReleases)
		}//if

		respObj?.releases?.each { release ->
			if (release.main_release)
				reqProcessor.getRelease(release.main_release, handleRelease)
			if (release.id)
				reqProcessor.getRelease(release.id, handleRelease)
		}// forEach

	}// handleArtistReleases


	def masterReleaseAlreadyProcessed = { releaseId ->
		def ret = db.MasterReleases.findOne(d_master_release_id: releaseId)
		if(ret)
			println "MasterRelease [${releaseId}] already processed!"
		!!ret
	}

	def releaseAlreadyProcessed = { releaseId ->
		def ret = db.Releases.findOne(d_release_id: releaseId)
		if(ret)
			println "Release [${releaseId}] already processed!"
		!!ret
	}

	def labelAlreadyProcessed = { labelId ->
		def ret = db.Labels.findOne(d_label_id: labelId)
		if(ret)
			println "Label [${labelId}] already processed!"
		!!ret
	}
	def artistAlreadyProcessed = { artistId ->
		def ret = db.Artists.findOne(d_artist_id: artistId)
		if(ret)
			println "Artist [${artistId}] already processed!"
		!!ret
	}

	def handleLabelReleases = { respObj ->
		def pagination = respObj.pagination;
		if (pagination?.urls?.next) {
			reqProcessor.getDiscogsData(GET, pagination.urls.next, [:], handleLabelReleases)
		}//if
		respObj.releases.each { release ->
			if (release.id && !releaseAlreadyProcessed(release.id))
				reqProcessor.getRelease(release.id, handleRelease)
		}// forEach
	}// handleLabelReleases

	def handleVersions = { versionsObj ->
		/// get the pagination frpm obj
		def pagination = versionsObj.pagination
		if (pagination?.urls?.next) {
			reqProcessor.getDiscogsData(GET, pagination.urls.next, [:], handleVersions)
		}

		/// get the list from the obj
		versionsObj.versions.each { ver ->
			if (ver.release_id && !releaseAlreadyProcessed(ver.release_id))
				reqProcessor.getRelease(ver.release_id, handleRelease)
		}// forEach
	}

	def releaseFilter = { criteria, releaseObj ->

		///// do the MUST-HAVEs
		def styles =  releaseObj.styles ?: []
		def genres = releaseObj.genres ?: []
		
		def isDisco = (styles.isEmpty() || !styles.intersect(["Soul", "Disco", "Funk","Rhythm & Blues"]).isEmpty()) && !genres.intersect(["Funk / Soul", "Electronic"]).isEmpty()
		def isJazzFunk = !styles.intersect(["Soul-Jazz", "Disco", "Jazz-Funk","Funk"]).isEmpty() &&	!genres.intersect(["Funk / Soul", "Electronic"]).isEmpty()
		def isFunkyStuff = isDisco || isJazzFunk

		///// do the exclusions
		def isRap = !styles.intersect([
			"Thug Rap",
			"Gansta",
			"Pop Rap",
			"Hip Hop",
			"RnB/Swing"
		]).isEmpty() ||
		!genres.intersect(["Hip Hop"]).isEmpty()

		def isCountry = !styles.intersect([
			"Country",
			"Schlager",
			"Country Rock"
		]).isEmpty()
		def isRock = !styles.intersect([
			"Heavy Metal",
			"Hard Rock",
			"Glam",
			"Arena Rock",
			"Rock & Roll"
		]).isEmpty() &&
		!genres.intersect(["Rock"]).isEmpty()

		return isFunkyStuff //|| (!(isRap || isCountry || isRock))
	}

	////////////////////////////////////////////////////////////////////////////
	//// Types stored to DB
	//// Types stored to DB
	//// Types stored to DB
	////////////////////////////////////////////////////////////////////////////
	def handleMasterRelease = { release ->
		if (!release)
			return

		def ds = db.MasterReleases.getCount()
		if (ds > 1024000)
			return

		if (releaseFilter([:], release) && !masterReleaseAlreadyProcessed(release.id)) {

			if (release.main_release) {
				if (!masterReleaseAlreadyProcessed(release.main_release))
					reqProcessor.getRelease(release.main_release, handleRelease)
			}
			if(release.versions_url) {
				reqProcessor.getDiscogsData(GET, release.versions_url, [:], handleVersions)
			}

			if(release.artists) {
				release.artists.each { art ->
					if (!artistAlreadyProcessed(art.id))
						reqProcessor.getArtist( art.id, handleArtist)
				}
			}
			/// write to db
			def doc = release;
			doc.d_master_release_id = doc.id
			def id = new ObjectId();
			doc.id = id
			def artist = doc?.artists[0]?.name ?: ""
			println("handleMasterRelease: [${doc.d_master_release_id}] $artist -> ${doc.title} : ${doc.genres}::${doc.styles}")
			db.MasterReleases.insert(doc);
		}//if
	}// handleMasterRelease

	def handleRelease = { release ->
		if ( !release )
			return

		def ds = db.Releases.getCount()
		if (ds > 307200)
			return

		if (releaseFilter([:], release) && !releaseAlreadyProcessed(release.id)) {

			if (release.main_release) {
				if (!releaseAlreadyProcessed(release.main_release))
					reqProcessor.getRelease(release.main_release)

			}

			if (release.companies) {
				release.companies.each { label ->
					if (!labelAlreadyProcessed(label.id))
						reqProcessor.getLabel(label.id, handleLabel)
				}
			}

			if (release.labels) {
				release.labels.each { label ->
					if (!labelAlreadyProcessed(label.id))
						reqProcessor.getLabel(label.id, handleLabel)
				}
			}

			if(release.extraartists) {
				release.extraartists.each { art ->
					if (!artistAlreadyProcessed(art.id))
						reqProcessor.getArtist( art.id, handleArtist)
				}
			}
			if(release.master_id) {
				if (!masterReleaseAlreadyProcessed(release.master_id))
					reqProcessor.getMasterRelease(release.master_id, handleMasterRelease)
			}
			/// write to db
			def doc = release;
			doc.community = null
			doc.d_release_id = doc.id
			def id = new ObjectId();
			doc.id = id
			//            println("handleRelease: " + doc.toString())
			def artist = doc?.artists[0]?.name ?: ""
			println("handleRelease: [${doc.d_release_id}] $artist -> ${doc.title} : ${doc.genres}::${doc.styles}")
			db.Releases.insert(doc);
		}//if
		else {
			def artist = release?.artists[0]?.name ?: ""
			println("handleRelease: SKIPPED [${release.id}] $artist -> $release.title} : ${release.genres}::${release.styles}")
		}
	}// handleRelease

	def handleLabel = { label ->
		if (!label)
			return

		def ds = db.Labels.getCount()
		if (ds > 10240)
			return

		if (!labelAlreadyProcessed(label.id)) {

			if (label.releases_url)
				reqProcessor.getDiscogsData(GET, label.releases_url,[:], handleLabelReleases)

			if (label.parent?.resource_url) {
				if(labelAlreadyProcessed(label.parent.id))
					reqProcessor.getDiscogsData(GET,label.parent.resource_url, [:],handleLabel)
			}

			if (label.sublabels) {
				label.sublabels.each { sub ->
					reqProcessor.getDiscogsData(GET, sub.resource_url, [:], handleLabel)
				}
			}

			/// write to db
			def doc = label;
			doc.d_label_id = doc.id
			def id = new ObjectId();
			doc.id = id
			doc.websites = doc.urls
			doc.urls = null

			println("handleLabel: ${doc.name}")
			db.Labels.insert(doc)
		}//if
	}// handleLabel

	def handleArtist = { artist ->
		if (!artist)
			return
		def ds = db.Artists.getCount()
		if (ds > 10240)
			return

		if (!artistAlreadyProcessed(artist.id)) {
			if (artist.releases_url)
				reqProcessor.getDiscogsData(GET, artist.releases_url, [:], handleArtistReleases)

			/// get the list from the obj
			artist.members.each { mbr ->
				if (mbr.id) {
					if(!artistAlreadyProcessed(mbr.id)) {
						reqProcessor.getArtist(mbr.id, handleArtist)
						reqProcessor.getArtistReleases(mbr.id, null, handleArtistReleases)
					}
				}
			}// forEach

			/// write to db
			def doc = artist
			doc.d_artist_id = doc.id
			def id = new ObjectId();
			doc.id = id
			doc.websites = doc.urls
			doc.urls = null
			doc.d_release_url = doc.release_url
			doc.release_url = null

			println("handleArtist: ${doc.name}")
			db.Artists.insert(doc)
		}//if
	};// handleArtist

	def start() {
		/// for now we are getting all releases from 3 to 30000

		Thread.start { reqProcessor.start() }
		Thread.sleep(200)
		def lst = ['9900','9904']
		//def lst = ['39357']
		lst.each {
			reqProcessor.getDiscogsData(GET, "/labels/$it", [:], handleLabel)
			Thread.sleep(12000)
		}
	}
}
