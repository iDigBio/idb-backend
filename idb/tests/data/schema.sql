--
-- PostgreSQL database dump
--

-- Dumped from database version 9.4.4
-- Dumped by pg_dump version 9.5.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

SET search_path = public, pg_catalog;

ALTER TABLE IF EXISTS ONLY public.uuids_siblings DROP CONSTRAINT IF EXISTS uuids_siblings_r2_fkey;
ALTER TABLE IF EXISTS ONLY public.uuids_siblings DROP CONSTRAINT IF EXISTS uuids_siblings_r1_fkey;
ALTER TABLE IF EXISTS ONLY public.uuids_identifier DROP CONSTRAINT IF EXISTS uuids_identifier_uuids_id_fkey;
ALTER TABLE IF EXISTS ONLY public.uuids_data DROP CONSTRAINT IF EXISTS uuids_data_uuids_id_fkey;
ALTER TABLE IF EXISTS ONLY public.uuids_data DROP CONSTRAINT IF EXISTS uuids_data_data_etag_fkey;
ALTER TABLE IF EXISTS ONLY public.recordsets DROP CONSTRAINT IF EXISTS recordsets_publisher_uuid_fkey;
ALTER TABLE IF EXISTS ONLY public.media_objects DROP CONSTRAINT IF EXISTS media_objects_url_fkey;
ALTER TABLE IF EXISTS ONLY public.media_objects DROP CONSTRAINT IF EXISTS media_objects_etag_fkey;
ALTER TABLE IF EXISTS ONLY public.annotations DROP CONSTRAINT IF EXISTS annotations_uuids_id_fkey;
DROP INDEX IF EXISTS public.uuids_type_parent;
DROP INDEX IF EXISTS public.uuids_siblings_r2;
DROP INDEX IF EXISTS public.uuids_siblings_r1;
DROP INDEX IF EXISTS public.uuids_parent;
DROP INDEX IF EXISTS public.uuids_identifier_uuids_id;
DROP INDEX IF EXISTS public.uuids_identifier_reverse_idx;
DROP INDEX IF EXISTS public.uuids_data_version;
DROP INDEX IF EXISTS public.uuids_data_uuids_id_modified;
DROP INDEX IF EXISTS public.uuids_data_modified;
DROP INDEX IF EXISTS public.media_objects_urls;
DROP INDEX IF EXISTS public.media_objects_etags;
DROP INDEX IF EXISTS public.idb_object_keys_etag;
DROP INDEX IF EXISTS public.data_riak_etag;
DROP INDEX IF EXISTS public.data_accessuris;
DROP INDEX IF EXISTS public.corrections_source;
ALTER TABLE IF EXISTS ONLY public.uuids_siblings DROP CONSTRAINT IF EXISTS uuids_siblings_pkey;
ALTER TABLE IF EXISTS ONLY public.uuids DROP CONSTRAINT IF EXISTS uuids_pkey;
ALTER TABLE IF EXISTS ONLY public.uuids_identifier DROP CONSTRAINT IF EXISTS uuids_identifier_pkey;
ALTER TABLE IF EXISTS ONLY public.uuids_identifier DROP CONSTRAINT IF EXISTS uuids_identifier_identifier_key;
ALTER TABLE IF EXISTS ONLY public.uuids_data DROP CONSTRAINT IF EXISTS uuids_data_pkey;
ALTER TABLE IF EXISTS ONLY public.recordsets DROP CONSTRAINT IF EXISTS recordsets_uuid_key;
ALTER TABLE IF EXISTS ONLY public.recordsets DROP CONSTRAINT IF EXISTS recordsets_pkey;
ALTER TABLE IF EXISTS ONLY public.publishers DROP CONSTRAINT IF EXISTS publishers_uuid_key;
ALTER TABLE IF EXISTS ONLY public.publishers DROP CONSTRAINT IF EXISTS publishers_pkey;
ALTER TABLE IF EXISTS ONLY public.objects DROP CONSTRAINT IF EXISTS objects_pkey;
ALTER TABLE IF EXISTS ONLY public.objects DROP CONSTRAINT IF EXISTS objects_etag_key;
ALTER TABLE IF EXISTS ONLY public.media DROP CONSTRAINT IF EXISTS media_url_key;
ALTER TABLE IF EXISTS ONLY public.media DROP CONSTRAINT IF EXISTS media_pkey;
ALTER TABLE IF EXISTS ONLY public.media_objects DROP CONSTRAINT IF EXISTS media_objects_pkey;
ALTER TABLE IF EXISTS ONLY public.recordsets DROP CONSTRAINT IF EXISTS idx_file_link_unique;
ALTER TABLE IF EXISTS ONLY public.idb_object_keys DROP CONSTRAINT IF EXISTS idb_object_keys_pkey;
ALTER TABLE IF EXISTS ONLY public.data DROP CONSTRAINT IF EXISTS data_pkey;
ALTER TABLE IF EXISTS ONLY public.corrections DROP CONSTRAINT IF EXISTS corrections_pkey;
ALTER TABLE IF EXISTS ONLY public.annotations DROP CONSTRAINT IF EXISTS annotations_pkey;
ALTER TABLE IF EXISTS public.uuids_siblings ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.uuids_identifier ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.uuids_data ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.recordsets ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.publishers ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.objects ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.media_objects ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.media ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.corrections ALTER COLUMN id DROP DEFAULT;
ALTER TABLE IF EXISTS public.annotations ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE IF EXISTS public.uuids_siblings_id_seq;
DROP SEQUENCE IF EXISTS public.uuids_identifier_id_seq;
DROP SEQUENCE IF EXISTS public.uuids_data_id_seq;
DROP SEQUENCE IF EXISTS public.recordsets_id_seq;
DROP TABLE IF EXISTS public.recordsets;
DROP SEQUENCE IF EXISTS public.publishers_id_seq;
DROP TABLE IF EXISTS public.publishers;
DROP SEQUENCE IF EXISTS public.objects_id_seq;
DROP TABLE IF EXISTS public.objects;
DROP SEQUENCE IF EXISTS public.media_objects_id_seq;
DROP TABLE IF EXISTS public.media_objects;
DROP SEQUENCE IF EXISTS public.media_id_seq;
DROP TABLE IF EXISTS public.media;
DROP VIEW IF EXISTS public.idigbio_uuids_new;
DROP VIEW IF EXISTS public.idigbio_uuids_data;
DROP TABLE IF EXISTS public.uuids_identifier;
DROP TABLE IF EXISTS public.uuids_data;
DROP VIEW IF EXISTS public.idigbio_relations;
DROP TABLE IF EXISTS public.uuids_siblings;
DROP TABLE IF EXISTS public.uuids;
DROP TABLE IF EXISTS public.idb_object_keys;
DROP TABLE IF EXISTS public.idb_api_keys;
DROP TABLE IF EXISTS public.data;
DROP SEQUENCE IF EXISTS public.corrections_id_seq;
DROP TABLE IF EXISTS public.corrections;
DROP SEQUENCE IF EXISTS public.annotations_id_seq;
DROP TABLE IF EXISTS public.annotations;


SET search_path = public, pg_catalog;

SET default_with_oids = false;

--
-- Name: annotations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE annotations (
    id bigint NOT NULL,
    uuids_id uuid NOT NULL,
    v jsonb NOT NULL,
    approved boolean DEFAULT false NOT NULL,
    source character varying(50) NOT NULL,
    updated_at timestamp without time zone DEFAULT now()
);


--
-- Name: annotations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE annotations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: annotations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE annotations_id_seq OWNED BY annotations.id;


--
-- Name: corrections; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE corrections (
    id bigint NOT NULL,
    k jsonb NOT NULL,
    v jsonb NOT NULL,
    approved boolean DEFAULT false NOT NULL,
    source character varying(50) NOT NULL,
    updated_at timestamp without time zone DEFAULT now()
);


--
-- Name: corrections_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE corrections_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: corrections_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE corrections_id_seq OWNED BY corrections.id;


--
-- Name: data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE data (
    etag character varying(41) NOT NULL,
    data jsonb,
    riak_etag character varying(41)
);


--
-- Name: idb_api_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE idb_api_keys (
    user_uuid uuid NOT NULL,
    apikey character varying(100) NOT NULL,
    objects_allowed boolean DEFAULT true NOT NULL,
    records_allowed boolean DEFAULT false NOT NULL,
    corrections_allowed boolean DEFAULT false NOT NULL,
    annotations_allowed boolean DEFAULT true NOT NULL
);


--
-- Name: idb_object_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE idb_object_keys (
    lookup_key character varying(255) NOT NULL,
    type character varying(100) NOT NULL,
    etag character varying(41) NOT NULL,
    date_modified timestamp without time zone DEFAULT now() NOT NULL,
    user_uuid character varying(40) NOT NULL
);


--
-- Name: uuids; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE uuids (
    id uuid NOT NULL,
    type character varying(50) NOT NULL,
    parent uuid,
    deleted boolean DEFAULT false NOT NULL
);


--
-- Name: uuids_siblings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE uuids_siblings (
    id bigint NOT NULL,
    r1 uuid NOT NULL,
    r2 uuid NOT NULL
);


--
-- Name: idigbio_relations; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW idigbio_relations AS
 SELECT a.r1 AS subject,
    uuids.type AS rel,
    a.r2 AS object
   FROM (( SELECT uuids_siblings.r1,
            uuids_siblings.r2
           FROM uuids_siblings
        UNION
         SELECT uuids_siblings.r2,
            uuids_siblings.r1
           FROM uuids_siblings) a
     JOIN uuids ON ((a.r2 = uuids.id)));


--
-- Name: uuids_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE uuids_data (
    id bigint NOT NULL,
    uuids_id uuid NOT NULL,
    data_etag character varying(41) NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL,
    version integer DEFAULT 0 NOT NULL
);


--
-- Name: uuids_identifier; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE uuids_identifier (
    id bigint NOT NULL,
    identifier text NOT NULL,
    uuids_id uuid NOT NULL
);


--
-- Name: idigbio_uuids_data; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW idigbio_uuids_data AS
 SELECT uuids.id AS uuid,
    uuids.type,
    uuids.deleted,
    latest.data_etag AS etag,
    latest.version,
    latest.modified,
    uuids.parent,
    ids.recordids,
    sibs.siblings,
    latest.id AS vid,
    data.data,
    data.riak_etag
   FROM ((((uuids
     LEFT JOIN LATERAL ( SELECT uuids_data.id,
            uuids_data.uuids_id,
            uuids_data.data_etag,
            uuids_data.modified,
            uuids_data.version
           FROM uuids_data
          WHERE (uuids_data.uuids_id = uuids.id)
          ORDER BY uuids_data.modified DESC
         LIMIT 1) latest ON ((latest.uuids_id = uuids.id)))
     LEFT JOIN LATERAL ( SELECT uuids_identifier.uuids_id,
            array_agg(uuids_identifier.identifier) AS recordids
           FROM uuids_identifier
          WHERE (uuids_identifier.uuids_id = uuids.id)
          GROUP BY uuids_identifier.uuids_id) ids ON ((ids.uuids_id = uuids.id)))
     LEFT JOIN LATERAL ( SELECT rels.subject,
            json_object_agg(rels.rel, rels.array_agg) AS siblings
           FROM ( SELECT rel_table.subject,
                    rel_table.rel,
                    array_agg(rel_table.object) AS array_agg
                   FROM ( SELECT rel_union.r1 AS subject,
                            uuids_1.type AS rel,
                            rel_union.r2 AS object
                           FROM (( SELECT uuids_siblings.r1,
                                    uuids_siblings.r2
                                   FROM uuids_siblings
                                UNION
                                 SELECT uuids_siblings.r2,
                                    uuids_siblings.r1
                                   FROM uuids_siblings) rel_union
                             JOIN uuids uuids_1 ON ((rel_union.r2 = uuids_1.id)))
                          WHERE (uuids_1.deleted = false)) rel_table
                  WHERE (rel_table.subject = uuids.id)
                  GROUP BY rel_table.subject, rel_table.rel) rels
          GROUP BY rels.subject) sibs ON ((sibs.subject = uuids.id)))
     LEFT JOIN data ON (((latest.data_etag)::text = (data.etag)::text)));


--
-- Name: idigbio_uuids_new; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW idigbio_uuids_new AS
 SELECT uuids.id AS uuid,
    uuids.type,
    uuids.deleted,
    latest.data_etag AS etag,
    latest.version,
    latest.modified,
    uuids.parent,
    ids.recordids,
    sibs.siblings,
    latest.id AS vid
   FROM (((uuids
     LEFT JOIN LATERAL ( SELECT uuids_data.id,
            uuids_data.uuids_id,
            uuids_data.data_etag,
            uuids_data.modified,
            uuids_data.version
           FROM uuids_data
          WHERE (uuids_data.uuids_id = uuids.id)
          ORDER BY uuids_data.modified DESC
         LIMIT 1) latest ON ((latest.uuids_id = uuids.id)))
     LEFT JOIN LATERAL ( SELECT uuids_identifier.uuids_id,
            array_agg(uuids_identifier.identifier) AS recordids
           FROM uuids_identifier
          WHERE (uuids_identifier.uuids_id = uuids.id)
          GROUP BY uuids_identifier.uuids_id) ids ON ((ids.uuids_id = uuids.id)))
     LEFT JOIN LATERAL ( SELECT rels.subject,
            json_object_agg(rels.rel, rels.array_agg) AS siblings
           FROM ( SELECT rel_table.subject,
                    rel_table.rel,
                    array_agg(rel_table.object) AS array_agg
                   FROM ( SELECT rel_union.r1 AS subject,
                            uuids_1.type AS rel,
                            rel_union.r2 AS object
                           FROM (( SELECT uuids_siblings.r1,
                                    uuids_siblings.r2
                                   FROM uuids_siblings
                                UNION
                                 SELECT uuids_siblings.r2,
                                    uuids_siblings.r1
                                   FROM uuids_siblings) rel_union
                             JOIN uuids uuids_1 ON ((rel_union.r2 = uuids_1.id)))
                          WHERE (uuids_1.deleted = false)) rel_table
                  WHERE (rel_table.subject = uuids.id)
                  GROUP BY rel_table.subject, rel_table.rel) rels
          GROUP BY rels.subject) sibs ON ((sibs.subject = uuids.id)));


--
-- Name: media; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE media (
    id bigint NOT NULL,
    url text,
    type character varying(20),
    mime character varying(255),
    last_status integer,
    last_check timestamp without time zone,
    owner uuid DEFAULT '872733a2-67a3-4c54-aa76-862735a5f334'::uuid
);


--
-- Name: media_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE media_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: media_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE media_id_seq OWNED BY media.id;


--
-- Name: media_objects; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE media_objects (
    id bigint NOT NULL,
    url text NOT NULL,
    etag character varying(41) NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL
);


--
-- Name: media_objects_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE media_objects_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: media_objects_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE media_objects_id_seq OWNED BY media_objects.id;


--
-- Name: objects; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE objects (
    id bigint NOT NULL,
    bucket character varying(255) NOT NULL,
    etag character varying(41) NOT NULL,
    detected_mime character varying(255),
    derivatives boolean DEFAULT false
);


--
-- Name: objects_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE objects_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: objects_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE objects_id_seq OWNED BY objects.id;


--
-- Name: publishers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE publishers (
    id bigint NOT NULL,
    uuid uuid,
    name text,
    recordids text[] DEFAULT '{}'::text[] NOT NULL,
    pub_type character varying(20) DEFAULT 'rss'::character varying NOT NULL,
    portal_url text,
    rss_url text NOT NULL,
    auto_publish boolean DEFAULT false NOT NULL,
    first_seen timestamp without time zone DEFAULT now() NOT NULL,
    last_seen timestamp without time zone DEFAULT now() NOT NULL,
    pub_date timestamp without time zone
);


--
-- Name: publishers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE publishers_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: publishers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE publishers_id_seq OWNED BY publishers.id;


--
-- Name: recordsets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE recordsets (
    id bigint NOT NULL,
    uuid uuid,
    publisher_uuid uuid,
    name text NOT NULL,
    recordids text[] DEFAULT '{}'::text[] NOT NULL,
    eml_link text,
    file_link text NOT NULL,
    ingest boolean DEFAULT false NOT NULL,
    first_seen timestamp without time zone DEFAULT now() NOT NULL,
    last_seen timestamp without time zone DEFAULT now() NOT NULL,
    pub_date timestamp without time zone,
    file_harvest_date timestamp without time zone,
    file_harvest_etag character varying(41),
    eml_harvest_date timestamp without time zone,
    eml_harvest_etag character varying(41)
);


--
-- Name: recordsets_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE recordsets_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: recordsets_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE recordsets_id_seq OWNED BY recordsets.id;


--
-- Name: uuids_data_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE uuids_data_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: uuids_data_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE uuids_data_id_seq OWNED BY uuids_data.id;


--
-- Name: uuids_identifier_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE uuids_identifier_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: uuids_identifier_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE uuids_identifier_id_seq OWNED BY uuids_identifier.id;


--
-- Name: uuids_siblings_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE uuids_siblings_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: uuids_siblings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE uuids_siblings_id_seq OWNED BY uuids_siblings.id;


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY annotations ALTER COLUMN id SET DEFAULT nextval('annotations_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY corrections ALTER COLUMN id SET DEFAULT nextval('corrections_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY media ALTER COLUMN id SET DEFAULT nextval('media_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY media_objects ALTER COLUMN id SET DEFAULT nextval('media_objects_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY objects ALTER COLUMN id SET DEFAULT nextval('objects_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY publishers ALTER COLUMN id SET DEFAULT nextval('publishers_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY recordsets ALTER COLUMN id SET DEFAULT nextval('recordsets_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_data ALTER COLUMN id SET DEFAULT nextval('uuids_data_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_identifier ALTER COLUMN id SET DEFAULT nextval('uuids_identifier_id_seq'::regclass);


--
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_siblings ALTER COLUMN id SET DEFAULT nextval('uuids_siblings_id_seq'::regclass);


--
-- Name: annotations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY annotations
    ADD CONSTRAINT annotations_pkey PRIMARY KEY (id);


--
-- Name: corrections_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY corrections
    ADD CONSTRAINT corrections_pkey PRIMARY KEY (id);


--
-- Name: data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY data
    ADD CONSTRAINT data_pkey PRIMARY KEY (etag);


--
-- Name: idb_object_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY idb_object_keys
    ADD CONSTRAINT idb_object_keys_pkey PRIMARY KEY (lookup_key);


--
-- Name: idx_file_link_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY recordsets
    ADD CONSTRAINT idx_file_link_unique UNIQUE (file_link);


--
-- Name: media_objects_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY media_objects
    ADD CONSTRAINT media_objects_pkey PRIMARY KEY (id);


--
-- Name: media_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY media
    ADD CONSTRAINT media_pkey PRIMARY KEY (id);


--
-- Name: media_url_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY media
    ADD CONSTRAINT media_url_key UNIQUE (url);


--
-- Name: objects_etag_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY objects
    ADD CONSTRAINT objects_etag_key UNIQUE (etag);


--
-- Name: objects_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY objects
    ADD CONSTRAINT objects_pkey PRIMARY KEY (id);


--
-- Name: publishers_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY publishers
    ADD CONSTRAINT publishers_pkey PRIMARY KEY (id);


--
-- Name: publishers_uuid_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY publishers
    ADD CONSTRAINT publishers_uuid_key UNIQUE (uuid);


--
-- Name: recordsets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY recordsets
    ADD CONSTRAINT recordsets_pkey PRIMARY KEY (id);


--
-- Name: recordsets_uuid_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY recordsets
    ADD CONSTRAINT recordsets_uuid_key UNIQUE (uuid);


--
-- Name: uuids_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_data
    ADD CONSTRAINT uuids_data_pkey PRIMARY KEY (id);


--
-- Name: uuids_identifier_identifier_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_identifier
    ADD CONSTRAINT uuids_identifier_identifier_key UNIQUE (identifier);


--
-- Name: uuids_identifier_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_identifier
    ADD CONSTRAINT uuids_identifier_pkey PRIMARY KEY (id);


--
-- Name: uuids_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids
    ADD CONSTRAINT uuids_pkey PRIMARY KEY (id);


--
-- Name: uuids_siblings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_siblings
    ADD CONSTRAINT uuids_siblings_pkey PRIMARY KEY (id);


--
-- Name: corrections_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX corrections_source ON corrections USING btree (source);


--
-- Name: data_accessuris; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX data_accessuris ON data USING btree ((COALESCE((data ->> 'ac:accessURI'::text), (data ->> 'ac:bestQualityAccessURI'::text), (data ->> 'dcterms:identifier'::text))));


--
-- Name: data_riak_etag; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX data_riak_etag ON data USING btree (riak_etag);


--
-- Name: idb_object_keys_etag; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idb_object_keys_etag ON idb_object_keys USING btree (etag);


--
-- Name: media_objects_etags; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX media_objects_etags ON media_objects USING btree (etag);


--
-- Name: media_objects_urls; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX media_objects_urls ON media_objects USING btree (url, modified DESC);


--
-- Name: uuids_data_modified; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX uuids_data_modified ON uuids_data USING btree (modified);


--
-- Name: uuids_data_uuids_id_modified; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX uuids_data_uuids_id_modified ON uuids_data USING btree (uuids_id, modified DESC);


--
-- Name: uuids_data_version; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX uuids_data_version ON uuids_data USING btree (version);


--
-- Name: uuids_identifier_reverse_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX uuids_identifier_reverse_idx ON uuids_identifier USING btree (reverse(identifier) text_pattern_ops);


--
-- Name: uuids_identifier_uuids_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX uuids_identifier_uuids_id ON uuids_identifier USING btree (uuids_id);


--
-- Name: uuids_parent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX uuids_parent ON uuids USING btree (parent);


--
-- Name: uuids_siblings_r1; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX uuids_siblings_r1 ON uuids_siblings USING btree (r1);


--
-- Name: uuids_siblings_r2; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX uuids_siblings_r2 ON uuids_siblings USING btree (r2);


--
-- Name: uuids_type_parent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX uuids_type_parent ON uuids USING btree (type, parent) WHERE (deleted = false);


--
-- Name: annotations_uuids_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY annotations
    ADD CONSTRAINT annotations_uuids_id_fkey FOREIGN KEY (uuids_id) REFERENCES uuids(id);


--
-- Name: media_objects_etag_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY media_objects
    ADD CONSTRAINT media_objects_etag_fkey FOREIGN KEY (etag) REFERENCES objects(etag) ON DELETE CASCADE;


--
-- Name: media_objects_url_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY media_objects
    ADD CONSTRAINT media_objects_url_fkey FOREIGN KEY (url) REFERENCES media(url) ON DELETE CASCADE;


--
-- Name: recordsets_publisher_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY recordsets
    ADD CONSTRAINT recordsets_publisher_uuid_fkey FOREIGN KEY (publisher_uuid) REFERENCES publishers(uuid);


--
-- Name: uuids_data_data_etag_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_data
    ADD CONSTRAINT uuids_data_data_etag_fkey FOREIGN KEY (data_etag) REFERENCES data(etag);


--
-- Name: uuids_data_uuids_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_data
    ADD CONSTRAINT uuids_data_uuids_id_fkey FOREIGN KEY (uuids_id) REFERENCES uuids(id);


--
-- Name: uuids_identifier_uuids_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_identifier
    ADD CONSTRAINT uuids_identifier_uuids_id_fkey FOREIGN KEY (uuids_id) REFERENCES uuids(id);


--
-- Name: uuids_siblings_r1_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_siblings
    ADD CONSTRAINT uuids_siblings_r1_fkey FOREIGN KEY (r1) REFERENCES uuids(id);


--
-- Name: uuids_siblings_r2_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_siblings
    ADD CONSTRAINT uuids_siblings_r2_fkey FOREIGN KEY (r2) REFERENCES uuids(id);


--
-- PostgreSQL database dump complete
--

