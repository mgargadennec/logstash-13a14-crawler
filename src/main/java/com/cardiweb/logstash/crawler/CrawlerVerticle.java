package com.cardiweb.logstash.crawler;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.collect.Maps;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.HttpResponseStatus;
import com.ning.http.client.Response;
import com.theoryinpractise.halbuilder.api.ContentRepresentation;
import com.theoryinpractise.halbuilder.api.Link;
import com.theoryinpractise.halbuilder.api.ReadableRepresentation;
import com.theoryinpractise.halbuilder.api.RepresentationException;
import com.theoryinpractise.halbuilder.api.RepresentationFactory;
import com.theoryinpractise.halbuilder.json.JsonRepresentationFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class CrawlerVerticle extends AbstractVerticle {
	private final static Logger logger = LoggerFactory.getLogger(CrawlerVerticle.class);
	private final static Map<Link, ContentRepresentation> visitedHref = Maps.newConcurrentMap();

	@Override
	public void start(Future<Void> future) throws InterruptedException, ExecutionException, IOException {
		RepresentationFactory rf = new JsonRepresentationFactory();

		AsyncHttpClient client = new AsyncHttpClient();
		Link link = new Link(rf, "root", "http://localhost:8081/");
		followLink(rf, client, link);
	}

	private void followLink(RepresentationFactory rf, AsyncHttpClient client, final Link link)
			throws InterruptedException {
		Thread.sleep(ThreadLocalRandom.current().nextLong(2000));
		String linkHref = link.getHref();
		if (link.hasTemplate()) {
			linkHref = link.getHref().substring(0, link.getHref().indexOf('{'));
		}

		client.prepareGet(linkHref).execute(new AsyncCompletionHandler<Response>() {
			@Override
			public com.ning.http.client.AsyncHandler.STATE onStatusReceived(HttpResponseStatus status)
					throws Exception {
				if(status.getStatusCode()!=200){
					return   STATE.ABORT;
				}
				return super.onStatusReceived(status);
			}
			
			@Override
			public Response onCompleted(Response response) throws Exception {
				String body = response.getResponseBody();
				try {
					ContentRepresentation representation = rf.readRepresentation(RepresentationFactory.HAL_JSON, new StringReader(body));
					logger.info(
							"Read representation for link " + link.getHref() + " --> " + representation.getContent());

					visitedHref.put(link, representation);

					Collection<Entry<String, ReadableRepresentation>> embeddeds = representation.getResources();
					for (Entry<String, ReadableRepresentation> embedded : embeddeds) {
						List<Link> links = embedded.getValue().getLinks();
						for (Link newLink : links) {
							if (visitedHref.containsKey(newLink)) {
								logger.info("Already visited " + newLink.getHref() + " --> "
										+ visitedHref.get(link).getContent());
							} else {
								followLink(rf, client, newLink);

							}
						}
					}

					List<Link> links = representation.getLinks();
					if (!links.isEmpty()) {
						for (Link newLink : links) {
							if (visitedHref.containsKey(newLink)) {
								logger.info("Already visited " + newLink.getHref() + " --> "
										+ visitedHref.get(link).getContent());
							} else {
								followLink(rf, client, newLink);

							}
						}
					}
				} catch (RepresentationException e) {
					logger.error("Error while converting result of link "+link.getHref()+" with body "+body,e);
				}
				return response;
			}

			@Override
			public void onThrowable(Throwable t) {
				System.err.println(t);
			}
		});
	}
}