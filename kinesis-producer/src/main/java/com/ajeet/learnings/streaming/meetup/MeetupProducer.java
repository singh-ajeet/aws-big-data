package com.ajeet.learnings.streaming.meetup;

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class MeetupProducer {
    private static final String MEETUP_ENDPOINT = "http://stream.meetup.com/2/rsvps";
    private static final String STREAM_NAME = "meetupRSVPEvents"
            ;
    private static final KinesisProducer getProducer(){
        KinesisProducerConfiguration config = new KinesisProducerConfiguration();
        return new KinesisProducer(config);
    }

    private static final JsonParser getMeetupJsonParser() throws IOException {
        URL url = new URL(MEETUP_ENDPOINT);

        URLConnection conn = url.openConnection();
        conn.addRequestProperty("User-Agent",
                "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0)");

        JsonFactory jsonFactory = new JsonFactory(new ObjectMapper());
        return jsonFactory.createParser(conn.getInputStream());
    }

    public static void main(String[] args) throws IOException {
        JsonParser parser = getMeetupJsonParser();
        KinesisProducer producer = getProducer();

        try {
            while (parser.nextToken()!=null)  {
                String record = parser.readValueAsTree().toString();
                producer.addUserRecord(STREAM_NAME, "RSVPEvent",  ByteBuffer.wrap(record.getBytes(StandardCharsets.UTF_8)));
            }
        } finally {
            try {
                parser.close();
                producer.flush();
            } catch (Exception ex){ }
            producer.destroy();
        }
    }
}
