--
-- PostgreSQL database dump
--

-- Dumped from database version 9.4.4
-- Dumped by pg_dump version 9.4.6
-- Started on 2016-03-29 13:22:39 EDT

SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- TOC entry 1 (class 3079 OID 2436812)
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;



SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 173 (class 1259 OID 2436817)
-- Name: annotations; Type: TABLE; Schema: public; Owner: -; Tablespace: 
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
-- TOC entry 174 (class 1259 OID 2436825)
-- Name: annotations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE annotations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 2223 (class 0 OID 0)
-- Dependencies: 174
-- Name: annotations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE annotations_id_seq OWNED BY annotations.id;


--
-- TOC entry 175 (class 1259 OID 2436827)
-- Name: corrections; Type: TABLE; Schema: public; Owner: -; Tablespace: 
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
-- TOC entry 176 (class 1259 OID 2436835)
-- Name: corrections_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE corrections_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 2224 (class 0 OID 0)
-- Dependencies: 176
-- Name: corrections_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE corrections_id_seq OWNED BY corrections.id;


--
-- TOC entry 177 (class 1259 OID 2436837)
-- Name: data; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE data (
    etag character varying(41) NOT NULL,
    data jsonb,
    riak_etag character varying(41)
);


--
-- TOC entry 178 (class 1259 OID 2436843)
-- Name: idb_api_keys; Type: TABLE; Schema: public; Owner: -; Tablespace: 
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
-- TOC entry 179 (class 1259 OID 2436850)
-- Name: idb_object_keys; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE idb_object_keys (
    lookup_key character varying(255) NOT NULL,
    type character varying(100) NOT NULL,
    etag character varying(41) NOT NULL,
    date_modified timestamp without time zone DEFAULT now() NOT NULL,
    user_uuid character varying(40) NOT NULL
);


--
-- TOC entry 180 (class 1259 OID 2436854)
-- Name: uuids; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE uuids (
    id uuid NOT NULL,
    type character varying(50) NOT NULL,
    parent uuid,
    deleted boolean DEFAULT false NOT NULL
);


--
-- TOC entry 181 (class 1259 OID 2436858)
-- Name: uuids_siblings; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE uuids_siblings (
    id bigint NOT NULL,
    r1 uuid NOT NULL,
    r2 uuid NOT NULL
);


--
-- TOC entry 182 (class 1259 OID 2436861)
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
-- TOC entry 183 (class 1259 OID 2436865)
-- Name: uuids_data; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE uuids_data (
    id bigint NOT NULL,
    uuids_id uuid NOT NULL,
    data_etag character varying(41) NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL,
    version integer DEFAULT 0 NOT NULL
);


--
-- TOC entry 184 (class 1259 OID 2436870)
-- Name: uuids_identifier; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE uuids_identifier (
    id bigint NOT NULL,
    identifier text NOT NULL,
    uuids_id uuid NOT NULL
);


--
-- TOC entry 185 (class 1259 OID 2436876)
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
-- TOC entry 186 (class 1259 OID 2436881)
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
-- TOC entry 187 (class 1259 OID 2436886)
-- Name: media; Type: TABLE; Schema: public; Owner: -; Tablespace: 
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
-- TOC entry 188 (class 1259 OID 2436893)
-- Name: media_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE media_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 2225 (class 0 OID 0)
-- Dependencies: 188
-- Name: media_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE media_id_seq OWNED BY media.id;


--
-- TOC entry 189 (class 1259 OID 2436895)
-- Name: media_objects; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE media_objects (
    id bigint NOT NULL,
    url text NOT NULL,
    etag character varying(41) NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL
);


--
-- TOC entry 190 (class 1259 OID 2436902)
-- Name: media_objects_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE media_objects_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 2226 (class 0 OID 0)
-- Dependencies: 190
-- Name: media_objects_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE media_objects_id_seq OWNED BY media_objects.id;


--
-- TOC entry 191 (class 1259 OID 2436904)
-- Name: objects; Type: TABLE; Schema: public; Owner: -; Tablespace: 
--

CREATE TABLE objects (
    id bigint NOT NULL,
    bucket character varying(255) NOT NULL,
    etag character varying(41) NOT NULL,
    detected_mime character varying(255),
    derivatives boolean DEFAULT false
);


--
-- TOC entry 192 (class 1259 OID 2436911)
-- Name: objects_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE objects_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 2227 (class 0 OID 0)
-- Dependencies: 192
-- Name: objects_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE objects_id_seq OWNED BY objects.id;


--
-- TOC entry 193 (class 1259 OID 2436913)
-- Name: publishers; Type: TABLE; Schema: public; Owner: -; Tablespace: 
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
-- TOC entry 194 (class 1259 OID 2436924)
-- Name: publishers_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE publishers_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 2228 (class 0 OID 0)
-- Dependencies: 194
-- Name: publishers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE publishers_id_seq OWNED BY publishers.id;


--
-- TOC entry 195 (class 1259 OID 2436926)
-- Name: recordsets; Type: TABLE; Schema: public; Owner: -; Tablespace: 
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
-- TOC entry 196 (class 1259 OID 2436936)
-- Name: recordsets_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE recordsets_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 2229 (class 0 OID 0)
-- Dependencies: 196
-- Name: recordsets_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE recordsets_id_seq OWNED BY recordsets.id;


--
-- TOC entry 197 (class 1259 OID 2436938)
-- Name: uuids_data_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE uuids_data_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 2230 (class 0 OID 0)
-- Dependencies: 197
-- Name: uuids_data_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE uuids_data_id_seq OWNED BY uuids_data.id;


--
-- TOC entry 198 (class 1259 OID 2436940)
-- Name: uuids_identifier_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE uuids_identifier_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 2231 (class 0 OID 0)
-- Dependencies: 198
-- Name: uuids_identifier_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE uuids_identifier_id_seq OWNED BY uuids_identifier.id;


--
-- TOC entry 199 (class 1259 OID 2436942)
-- Name: uuids_siblings_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE uuids_siblings_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- TOC entry 2232 (class 0 OID 0)
-- Dependencies: 199
-- Name: uuids_siblings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE uuids_siblings_id_seq OWNED BY uuids_siblings.id;


--
-- TOC entry 2010 (class 2604 OID 2436944)
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY annotations ALTER COLUMN id SET DEFAULT nextval('annotations_id_seq'::regclass);


--
-- TOC entry 2013 (class 2604 OID 2436945)
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY corrections ALTER COLUMN id SET DEFAULT nextval('corrections_id_seq'::regclass);


--
-- TOC entry 2026 (class 2604 OID 2436946)
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY media ALTER COLUMN id SET DEFAULT nextval('media_id_seq'::regclass);


--
-- TOC entry 2028 (class 2604 OID 2436947)
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY media_objects ALTER COLUMN id SET DEFAULT nextval('media_objects_id_seq'::regclass);


--
-- TOC entry 2030 (class 2604 OID 2436948)
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY objects ALTER COLUMN id SET DEFAULT nextval('objects_id_seq'::regclass);


--
-- TOC entry 2036 (class 2604 OID 2436949)
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY publishers ALTER COLUMN id SET DEFAULT nextval('publishers_id_seq'::regclass);


--
-- TOC entry 2041 (class 2604 OID 2436950)
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY recordsets ALTER COLUMN id SET DEFAULT nextval('recordsets_id_seq'::regclass);


--
-- TOC entry 2023 (class 2604 OID 2436951)
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_data ALTER COLUMN id SET DEFAULT nextval('uuids_data_id_seq'::regclass);


--
-- TOC entry 2024 (class 2604 OID 2436952)
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_identifier ALTER COLUMN id SET DEFAULT nextval('uuids_identifier_id_seq'::regclass);


--
-- TOC entry 2020 (class 2604 OID 2436953)
-- Name: id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_siblings ALTER COLUMN id SET DEFAULT nextval('uuids_siblings_id_seq'::regclass);


--
-- TOC entry 2043 (class 2606 OID 2436955)
-- Name: annotations_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY annotations
    ADD CONSTRAINT annotations_pkey PRIMARY KEY (id);


--
-- TOC entry 2045 (class 2606 OID 2436957)
-- Name: corrections_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY corrections
    ADD CONSTRAINT corrections_pkey PRIMARY KEY (id);


--
-- TOC entry 2049 (class 2606 OID 2436959)
-- Name: data_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY data
    ADD CONSTRAINT data_pkey PRIMARY KEY (etag);


--
-- TOC entry 2053 (class 2606 OID 2436961)
-- Name: idb_object_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY idb_object_keys
    ADD CONSTRAINT idb_object_keys_pkey PRIMARY KEY (lookup_key);


--
-- TOC entry 2080 (class 2606 OID 2436963)
-- Name: media_objects_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY media_objects
    ADD CONSTRAINT media_objects_pkey PRIMARY KEY (id);


--
-- TOC entry 2075 (class 2606 OID 2436965)
-- Name: media_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY media
    ADD CONSTRAINT media_pkey PRIMARY KEY (id);


--
-- TOC entry 2077 (class 2606 OID 2436967)
-- Name: media_url_key; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY media
    ADD CONSTRAINT media_url_key UNIQUE (url);


--
-- TOC entry 2083 (class 2606 OID 2436969)
-- Name: objects_etag_key; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY objects
    ADD CONSTRAINT objects_etag_key UNIQUE (etag);


--
-- TOC entry 2085 (class 2606 OID 2436971)
-- Name: objects_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY objects
    ADD CONSTRAINT objects_pkey PRIMARY KEY (id);


--
-- TOC entry 2087 (class 2606 OID 2436973)
-- Name: publishers_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY publishers
    ADD CONSTRAINT publishers_pkey PRIMARY KEY (id);


--
-- TOC entry 2089 (class 2606 OID 2436975)
-- Name: publishers_uuid_key; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY publishers
    ADD CONSTRAINT publishers_uuid_key UNIQUE (uuid);


--
-- TOC entry 2091 (class 2606 OID 2436977)
-- Name: recordsets_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY recordsets
    ADD CONSTRAINT recordsets_pkey PRIMARY KEY (id);


--
-- TOC entry 2093 (class 2606 OID 2436979)
-- Name: recordsets_uuid_key; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY recordsets
    ADD CONSTRAINT recordsets_uuid_key UNIQUE (uuid);


--
-- TOC entry 2065 (class 2606 OID 2436981)
-- Name: uuids_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY uuids_data
    ADD CONSTRAINT uuids_data_pkey PRIMARY KEY (id);


--
-- TOC entry 2069 (class 2606 OID 2436983)
-- Name: uuids_identifier_identifier_key; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY uuids_identifier
    ADD CONSTRAINT uuids_identifier_identifier_key UNIQUE (identifier);


--
-- TOC entry 2071 (class 2606 OID 2436985)
-- Name: uuids_identifier_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY uuids_identifier
    ADD CONSTRAINT uuids_identifier_pkey PRIMARY KEY (id);


--
-- TOC entry 2057 (class 2606 OID 2436987)
-- Name: uuids_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY uuids
    ADD CONSTRAINT uuids_pkey PRIMARY KEY (id);


--
-- TOC entry 2060 (class 2606 OID 2436989)
-- Name: uuids_siblings_pkey; Type: CONSTRAINT; Schema: public; Owner: -; Tablespace: 
--

ALTER TABLE ONLY uuids_siblings
    ADD CONSTRAINT uuids_siblings_pkey PRIMARY KEY (id);


--
-- TOC entry 2046 (class 1259 OID 2436990)
-- Name: corrections_source; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX corrections_source ON corrections USING btree (source);


--
-- TOC entry 2047 (class 1259 OID 2436991)
-- Name: data_accessuris; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX data_accessuris ON data USING btree ((COALESCE((data ->> 'ac:accessURI'::text), (data ->> 'ac:bestQualityAccessURI'::text), (data ->> 'dcterms:identifier'::text))));


--
-- TOC entry 2050 (class 1259 OID 2436992)
-- Name: data_riak_etag; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX data_riak_etag ON data USING btree (riak_etag);


--
-- TOC entry 2051 (class 1259 OID 2436993)
-- Name: idb_object_keys_etag; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX idb_object_keys_etag ON idb_object_keys USING btree (etag);


--
-- TOC entry 2078 (class 1259 OID 2436994)
-- Name: media_objects_etags; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX media_objects_etags ON media_objects USING btree (etag);


--
-- TOC entry 2081 (class 1259 OID 2436995)
-- Name: media_objects_urls; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX media_objects_urls ON media_objects USING btree (url);


--
-- TOC entry 2063 (class 1259 OID 2436996)
-- Name: uuids_data_modified; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX uuids_data_modified ON uuids_data USING btree (modified);


--
-- TOC entry 2066 (class 1259 OID 2436997)
-- Name: uuids_data_uuids_id; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX uuids_data_uuids_id ON uuids_data USING btree (uuids_id);


--
-- TOC entry 2067 (class 1259 OID 2436998)
-- Name: uuids_data_version; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX uuids_data_version ON uuids_data USING btree (version);


--
-- TOC entry 2054 (class 1259 OID 2436999)
-- Name: uuids_deleted; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX uuids_deleted ON uuids USING btree (deleted);


--
-- TOC entry 2072 (class 1259 OID 2437000)
-- Name: uuids_identifier_reverse_idx; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX uuids_identifier_reverse_idx ON uuids_identifier USING btree (reverse(identifier) text_pattern_ops);


--
-- TOC entry 2073 (class 1259 OID 2437001)
-- Name: uuids_identifier_uuids_id; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX uuids_identifier_uuids_id ON uuids_identifier USING btree (uuids_id);


--
-- TOC entry 2055 (class 1259 OID 2437002)
-- Name: uuids_parent; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX uuids_parent ON uuids USING btree (parent);


--
-- TOC entry 2061 (class 1259 OID 2437003)
-- Name: uuids_siblings_r1; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX uuids_siblings_r1 ON uuids_siblings USING btree (r1);


--
-- TOC entry 2062 (class 1259 OID 2437004)
-- Name: uuids_siblings_r2; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX uuids_siblings_r2 ON uuids_siblings USING btree (r2);


--
-- TOC entry 2058 (class 1259 OID 2437005)
-- Name: uuids_type; Type: INDEX; Schema: public; Owner: -; Tablespace: 
--

CREATE INDEX uuids_type ON uuids USING btree (type);


--
-- TOC entry 2094 (class 2606 OID 2437006)
-- Name: annotations_uuids_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY annotations
    ADD CONSTRAINT annotations_uuids_id_fkey FOREIGN KEY (uuids_id) REFERENCES uuids(id);


--
-- TOC entry 2100 (class 2606 OID 2437011)
-- Name: media_objects_etag_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY media_objects
    ADD CONSTRAINT media_objects_etag_fkey FOREIGN KEY (etag) REFERENCES objects(etag);


--
-- TOC entry 2101 (class 2606 OID 2437016)
-- Name: media_objects_url_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY media_objects
    ADD CONSTRAINT media_objects_url_fkey FOREIGN KEY (url) REFERENCES media(url);


--
-- TOC entry 2102 (class 2606 OID 2437021)
-- Name: recordsets_publisher_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY recordsets
    ADD CONSTRAINT recordsets_publisher_uuid_fkey FOREIGN KEY (publisher_uuid) REFERENCES publishers(uuid);


--
-- TOC entry 2097 (class 2606 OID 2437026)
-- Name: uuids_data_data_etag_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_data
    ADD CONSTRAINT uuids_data_data_etag_fkey FOREIGN KEY (data_etag) REFERENCES data(etag);


--
-- TOC entry 2098 (class 2606 OID 2437031)
-- Name: uuids_data_uuids_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_data
    ADD CONSTRAINT uuids_data_uuids_id_fkey FOREIGN KEY (uuids_id) REFERENCES uuids(id);


--
-- TOC entry 2099 (class 2606 OID 2437036)
-- Name: uuids_identifier_uuids_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_identifier
    ADD CONSTRAINT uuids_identifier_uuids_id_fkey FOREIGN KEY (uuids_id) REFERENCES uuids(id);


--
-- TOC entry 2095 (class 2606 OID 2437041)
-- Name: uuids_siblings_r1_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_siblings
    ADD CONSTRAINT uuids_siblings_r1_fkey FOREIGN KEY (r1) REFERENCES uuids(id);


--
-- TOC entry 2096 (class 2606 OID 2437046)
-- Name: uuids_siblings_r2_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY uuids_siblings
    ADD CONSTRAINT uuids_siblings_r2_fkey FOREIGN KEY (r2) REFERENCES uuids(id);


--
-- TOC entry 2221 (class 0 OID 0)
-- Dependencies: 7
-- Name: public; Type: ACL; Schema: -; Owner: -
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


-- Completed on 2016-03-29 13:22:39 EDT

--
-- PostgreSQL database dump complete
--

