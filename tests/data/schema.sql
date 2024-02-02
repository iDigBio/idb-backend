--
-- PostgreSQL database dump
--

-- Dumped from database version 14.7 (Ubuntu 14.7-1.pgdg20.04+1)
-- Dumped by pg_dump version 14.7 (Ubuntu 14.7-1.pgdg20.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

ALTER TABLE ONLY public.uuids_siblings DROP CONSTRAINT uuids_siblings_r2_fkey;
ALTER TABLE ONLY public.uuids_siblings DROP CONSTRAINT uuids_siblings_r1_fkey;
ALTER TABLE ONLY public.uuids_identifier DROP CONSTRAINT uuids_identifier_uuids_id_fkey;
ALTER TABLE ONLY public.uuids_data DROP CONSTRAINT uuids_data_uuids_id_fkey;
ALTER TABLE ONLY public.uuids_data DROP CONSTRAINT uuids_data_data_etag_fkey;
ALTER TABLE ONLY public.recordsets DROP CONSTRAINT recordsets_publisher_uuid_fkey;
ALTER TABLE ONLY public.media_objects DROP CONSTRAINT media_objects_url_fkey;
ALTER TABLE ONLY public.media_objects DROP CONSTRAINT media_objects_etag_fkey;
ALTER TABLE ONLY public.annotations DROP CONSTRAINT annotations_uuids_id_fkey;
DROP TRIGGER trigger_set_ingest_paused_date ON public.recordsets;
DROP INDEX public.uuids_type_parent;
DROP INDEX public.uuids_siblings_r2;
DROP INDEX public.uuids_siblings_r1;
DROP INDEX public.uuids_parent;
DROP INDEX public.uuids_identifier_uuids_id;
DROP INDEX public.uuids_identifier_reverse_idx;
DROP INDEX public.uuids_data_version;
DROP INDEX public.uuids_data_uuids_id_modified;
DROP INDEX public.uuids_data_data_etag_idx;
DROP INDEX public.media_objects_urls;
DROP INDEX public.media_objects_etags;
DROP INDEX public.index_ceph_on_filename_with_pattern_ops;
DROP INDEX public.idb_object_keys_etag;
DROP INDEX public.corrections_source;
DROP INDEX public.ceph_objects_bucket_name;
ALTER TABLE ONLY public.uuids_siblings DROP CONSTRAINT uuids_siblings_pkey;
ALTER TABLE ONLY public.uuids DROP CONSTRAINT uuids_pkey;
ALTER TABLE ONLY public.uuids_identifier DROP CONSTRAINT uuids_identifier_pkey;
ALTER TABLE ONLY public.uuids_identifier DROP CONSTRAINT uuids_identifier_identifier_key;
ALTER TABLE ONLY public.uuids_data DROP CONSTRAINT uuids_data_pkey;
ALTER TABLE ONLY public.recordsets DROP CONSTRAINT recordsets_uuid_key;
ALTER TABLE ONLY public.recordsets DROP CONSTRAINT recordsets_pkey;
ALTER TABLE ONLY public.publishers DROP CONSTRAINT publishers_uuid_key;
ALTER TABLE ONLY public.publishers DROP CONSTRAINT publishers_pkey;
ALTER TABLE ONLY public.objects DROP CONSTRAINT objects_pkey;
ALTER TABLE ONLY public.objects DROP CONSTRAINT objects_etag_key;
ALTER TABLE ONLY public.media DROP CONSTRAINT media_url_key;
ALTER TABLE ONLY public.media DROP CONSTRAINT media_pkey;
ALTER TABLE ONLY public.media_objects DROP CONSTRAINT media_objects_pkey;
ALTER TABLE ONLY public.recordsets DROP CONSTRAINT idx_file_link_unique;
ALTER TABLE ONLY public.idb_object_keys DROP CONSTRAINT idb_object_keys_pkey;
ALTER TABLE ONLY public.data DROP CONSTRAINT data_pkey;
ALTER TABLE ONLY public.corrections DROP CONSTRAINT corrections_pkey;
ALTER TABLE ONLY public.annotations DROP CONSTRAINT annotations_source_id_key;
ALTER TABLE ONLY public.annotations DROP CONSTRAINT annotations_pkey;
ALTER TABLE public.uuids_siblings ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.uuids_identifier ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.uuids_data ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.recordsets ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.publishers ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.objects ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.media_objects ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.media ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.corrections ALTER COLUMN id DROP DEFAULT;
ALTER TABLE public.annotations ALTER COLUMN id DROP DEFAULT;
DROP SEQUENCE public.uuids_siblings_id_seq;
DROP SEQUENCE public.uuids_identifier_id_seq;
DROP SEQUENCE public.uuids_data_id_seq;
DROP SEQUENCE public.recordsets_id_seq;
DROP TABLE public.recordsets;
DROP SEQUENCE public.publishers_id_seq;
DROP TABLE public.publishers;
DROP SEQUENCE public.objects_id_seq;
DROP TABLE public.objects;
DROP SEQUENCE public.media_objects_id_seq;
DROP TABLE public.media_objects;
DROP SEQUENCE public.media_id_seq;
DROP TABLE public.media;
DROP VIEW public.idigbio_uuids_test;
DROP VIEW public.idigbio_uuids_new_ron;
DROP VIEW public.idigbio_uuids_new;
DROP VIEW public.idigbio_uuids_data;
DROP TABLE public.uuids_identifier;
DROP TABLE public.uuids_data;
DROP VIEW public.idigbio_relations;
DROP TABLE public.uuids_siblings;
DROP TABLE public.uuids;
DROP TABLE public.idb_object_keys;
DROP TABLE public.idb_api_keys;
DROP TABLE public.deleted_from_uuids_identifier;
DROP TABLE public.data;
DROP SEQUENCE public.corrections_id_seq;
DROP TABLE public.corrections;
DROP TABLE public.ceph_server_files;
DROP TABLE public.ceph_objects;
DROP SEQUENCE public.annotations_id_seq;
DROP TABLE public.annotations;
DROP FUNCTION public.set_ingest_paused_date();
DROP EXTENSION intarray;
DROP EXTENSION amcheck;
--
-- Name: amcheck; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS amcheck WITH SCHEMA public;


--
-- Name: EXTENSION amcheck; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION amcheck IS 'functions for verifying relation integrity';


--
-- Name: intarray; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS intarray WITH SCHEMA public;


--
-- Name: EXTENSION intarray; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION intarray IS 'functions, operators, and index support for 1-D arrays of integers';


--
-- Name: set_ingest_paused_date(); Type: FUNCTION; Schema: public; Owner: idigbio
--

CREATE FUNCTION public.set_ingest_paused_date() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
IF OLD.ingest_is_paused=true and NEW.ingest_is_paused=false THEN
  NEW.ingest_paused_date = NULL;
ELSIF OLD.ingest_is_paused=false and NEW.ingest_is_paused=true THEN
 NEW.ingest_paused_date = now();
END IF;
RETURN NEW;
END;
$$;


ALTER FUNCTION public.set_ingest_paused_date() OWNER TO idigbio;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: annotations; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.annotations (
    id bigint NOT NULL,
    source_id uuid NOT NULL,
    uuids_id uuid NOT NULL,
    v jsonb NOT NULL,
    approved boolean DEFAULT false NOT NULL,
    source character varying(50) NOT NULL,
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.annotations OWNER TO idigbio;

--
-- Name: annotations_id_seq; Type: SEQUENCE; Schema: public; Owner: idigbio
--

CREATE SEQUENCE public.annotations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.annotations_id_seq OWNER TO idigbio;

--
-- Name: annotations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: idigbio
--

ALTER SEQUENCE public.annotations_id_seq OWNED BY public.annotations.id;


--
-- Name: ceph_objects; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.ceph_objects (
    ceph_bucket character varying(32) NOT NULL,
    ceph_name character varying(128) NOT NULL,
    ceph_date timestamp without time zone,
    ceph_bytes bigint,
    ceph_etag uuid,
    tsm_eligible boolean,
    tsm_status character varying(16),
    tsm_last_success timestamp without time zone,
    tsm_last_failure timestamp without time zone,
    tsm_bytes bigint,
    tsm_path character varying(32),
    ver_status character varying(16),
    ver_last_success timestamp without time zone,
    ver_last_failure timestamp without time zone,
    rest_status character varying(16),
    rest_last_success timestamp without time zone,
    rest_last_failure timestamp without time zone,
    notes character varying(16),
    ceph_deleted_date timestamp without time zone,
    ceph_deleted boolean DEFAULT false NOT NULL
);


ALTER TABLE public.ceph_objects OWNER TO idigbio;

--
-- Name: ceph_server_files; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.ceph_server_files (
    server character varying(16) NOT NULL,
    line integer,
    unk integer,
    perms character varying(16),
    unk2 integer,
    owner_name character varying(16),
    group_name character varying(16),
    size bigint,
    day integer,
    month character varying(3),
    year_time character varying(8),
    fullname text NOT NULL,
    filename text NOT NULL
);


ALTER TABLE public.ceph_server_files OWNER TO idigbio;

--
-- Name: corrections; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.corrections (
    id bigint NOT NULL,
    k jsonb NOT NULL,
    v jsonb NOT NULL,
    approved boolean DEFAULT false NOT NULL,
    source character varying(50) NOT NULL,
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.corrections OWNER TO idigbio;

--
-- Name: corrections_id_seq; Type: SEQUENCE; Schema: public; Owner: idigbio
--

CREATE SEQUENCE public.corrections_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.corrections_id_seq OWNER TO idigbio;

--
-- Name: corrections_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: idigbio
--

ALTER SEQUENCE public.corrections_id_seq OWNED BY public.corrections.id;


--
-- Name: data; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.data (
    etag character varying(41) NOT NULL,
    data jsonb,
    riak_etag character varying(41)
);


ALTER TABLE public.data OWNER TO idigbio;

--
-- Name: deleted_from_uuids_identifier; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.deleted_from_uuids_identifier (
    id bigint,
    identifier text,
    uuids_id uuid
);


ALTER TABLE public.deleted_from_uuids_identifier OWNER TO idigbio;

--
-- Name: idb_api_keys; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.idb_api_keys (
    user_uuid uuid NOT NULL,
    apikey character varying(100) NOT NULL,
    objects_allowed boolean DEFAULT true NOT NULL,
    records_allowed boolean DEFAULT false NOT NULL,
    corrections_allowed boolean DEFAULT false NOT NULL,
    annotations_allowed boolean DEFAULT true NOT NULL
);


ALTER TABLE public.idb_api_keys OWNER TO idigbio;

--
-- Name: idb_object_keys; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.idb_object_keys (
    lookup_key character varying(255) NOT NULL,
    type character varying(100) NOT NULL,
    etag character varying(41) NOT NULL,
    date_modified timestamp without time zone DEFAULT now() NOT NULL,
    user_uuid character varying(40) NOT NULL
);


ALTER TABLE public.idb_object_keys OWNER TO idigbio;

--
-- Name: uuids; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.uuids (
    id uuid NOT NULL,
    type character varying(50) NOT NULL,
    parent uuid,
    deleted boolean DEFAULT false NOT NULL
);


ALTER TABLE public.uuids OWNER TO idigbio;

--
-- Name: uuids_siblings; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.uuids_siblings (
    id bigint NOT NULL,
    r1 uuid NOT NULL,
    r2 uuid NOT NULL
);


ALTER TABLE public.uuids_siblings OWNER TO idigbio;

--
-- Name: idigbio_relations; Type: VIEW; Schema: public; Owner: idigbio
--

CREATE VIEW public.idigbio_relations AS
 SELECT a.r1 AS subject,
    uuids.type AS rel,
    a.r2 AS object
   FROM (( SELECT uuids_siblings.r1,
            uuids_siblings.r2
           FROM public.uuids_siblings
        UNION
         SELECT uuids_siblings.r2,
            uuids_siblings.r1
           FROM public.uuids_siblings) a
     JOIN public.uuids ON ((a.r2 = uuids.id)));


ALTER TABLE public.idigbio_relations OWNER TO idigbio;

--
-- Name: uuids_data; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.uuids_data (
    id bigint NOT NULL,
    uuids_id uuid NOT NULL,
    data_etag character varying(41) NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL,
    version integer DEFAULT 0 NOT NULL
);


ALTER TABLE public.uuids_data OWNER TO idigbio;

--
-- Name: uuids_identifier; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.uuids_identifier (
    id bigint NOT NULL,
    identifier text NOT NULL,
    uuids_id uuid NOT NULL
);


ALTER TABLE public.uuids_identifier OWNER TO idigbio;

--
-- Name: idigbio_uuids_data; Type: VIEW; Schema: public; Owner: idigbio
--

CREATE VIEW public.idigbio_uuids_data AS
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
    data.data
   FROM ((((public.uuids
     LEFT JOIN LATERAL ( SELECT uuids_data.id,
            uuids_data.uuids_id,
            uuids_data.data_etag,
            uuids_data.modified,
            uuids_data.version
           FROM public.uuids_data
          WHERE (uuids_data.uuids_id = uuids.id)
          ORDER BY uuids_data.modified DESC
         LIMIT 1) latest ON (true))
     LEFT JOIN LATERAL ( SELECT array_agg(uuids_identifier.identifier) AS recordids
           FROM public.uuids_identifier
          WHERE (uuids_identifier.uuids_id = uuids.id)) ids ON (true))
     LEFT JOIN LATERAL ( SELECT json_object_agg(rels.rel, rels.array_agg) AS siblings
           FROM ( SELECT sibs_1.type AS rel,
                    array_agg(rel_union.r2) AS array_agg
                   FROM (( SELECT uuids_siblings.r1,
                            uuids_siblings.r2
                           FROM public.uuids_siblings
                        UNION
                         SELECT uuids_siblings.r2,
                            uuids_siblings.r1
                           FROM public.uuids_siblings) rel_union
                     JOIN public.uuids sibs_1 ON ((rel_union.r2 = sibs_1.id)))
                  WHERE ((sibs_1.deleted = false) AND (rel_union.r1 = uuids.id))
                  GROUP BY sibs_1.type) rels) sibs ON (true))
     LEFT JOIN public.data ON (((latest.data_etag)::text = (data.etag)::text)));


ALTER TABLE public.idigbio_uuids_data OWNER TO idigbio;

--
-- Name: idigbio_uuids_new; Type: VIEW; Schema: public; Owner: idigbio
--

CREATE VIEW public.idigbio_uuids_new AS
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
   FROM (((public.uuids
     LEFT JOIN LATERAL ( SELECT uuids_data.id,
            uuids_data.uuids_id,
            uuids_data.data_etag,
            uuids_data.modified,
            uuids_data.version
           FROM public.uuids_data
          WHERE (uuids_data.uuids_id = uuids.id)
          ORDER BY uuids_data.modified DESC
         LIMIT 1) latest ON (true))
     LEFT JOIN LATERAL ( SELECT array_agg(uuids_identifier.identifier) AS recordids
           FROM public.uuids_identifier
          WHERE (uuids_identifier.uuids_id = uuids.id)) ids ON (true))
     LEFT JOIN LATERAL ( SELECT json_object_agg(rels.rel, rels.array_agg) AS siblings
           FROM ( SELECT sibs_1.type AS rel,
                    array_agg(rel_union.r2) AS array_agg
                   FROM (( SELECT uuids_siblings.r1,
                            uuids_siblings.r2
                           FROM public.uuids_siblings
                        UNION
                         SELECT uuids_siblings.r2,
                            uuids_siblings.r1
                           FROM public.uuids_siblings) rel_union
                     JOIN public.uuids sibs_1 ON ((rel_union.r2 = sibs_1.id)))
                  WHERE ((sibs_1.deleted = false) AND (rel_union.r1 = uuids.id))
                  GROUP BY sibs_1.type) rels) sibs ON (true));


ALTER TABLE public.idigbio_uuids_new OWNER TO idigbio;

--
-- Name: idigbio_uuids_new_ron; Type: VIEW; Schema: public; Owner: idigbio
--

CREATE VIEW public.idigbio_uuids_new_ron AS
 SELECT uuids.id AS uuid,
    uuids.type,
    uuids.deleted,
    latest.data_etag AS etag,
    latest.version,
    latest.modified,
    uuids.parent,
    ids.recordids,
    latest.id AS vid
   FROM ((public.uuids
     LEFT JOIN LATERAL ( SELECT uuids_data.id,
            uuids_data.uuids_id,
            uuids_data.data_etag,
            uuids_data.modified,
            uuids_data.version
           FROM public.uuids_data
          WHERE (uuids_data.uuids_id = uuids.id)
          ORDER BY uuids_data.modified DESC
         LIMIT 1) latest ON (true))
     LEFT JOIN LATERAL ( SELECT array_agg(uuids_identifier.identifier) AS recordids
           FROM public.uuids_identifier
          WHERE (uuids_identifier.uuids_id = uuids.id)) ids ON (true));


ALTER TABLE public.idigbio_uuids_new_ron OWNER TO idigbio;

--
-- Name: idigbio_uuids_test; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.idigbio_uuids_test AS
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
    ac.annotation_count,
    data.data
   FROM (((((public.uuids
     LEFT JOIN LATERAL ( SELECT uuids_data.id,
            uuids_data.uuids_id,
            uuids_data.data_etag,
            uuids_data.modified,
            uuids_data.version
           FROM public.uuids_data
          WHERE (uuids_data.uuids_id = uuids.id)
          ORDER BY uuids_data.modified DESC
         LIMIT 1) latest ON (true))
     LEFT JOIN LATERAL ( SELECT array_agg(uuids_identifier.identifier) AS recordids
           FROM public.uuids_identifier
          WHERE (uuids_identifier.uuids_id = uuids.id)) ids ON (true))
     LEFT JOIN LATERAL ( SELECT json_object_agg(rels.rel, rels.array_agg) AS siblings
           FROM ( SELECT sibs_1.type AS rel,
                    array_agg(rel_union.r2) AS array_agg
                   FROM (( SELECT uuids_siblings.r1,
                            uuids_siblings.r2
                           FROM public.uuids_siblings
                        UNION
                         SELECT uuids_siblings.r2,
                            uuids_siblings.r1
                           FROM public.uuids_siblings) rel_union
                     JOIN public.uuids sibs_1 ON ((rel_union.r2 = sibs_1.id)))
                  WHERE ((sibs_1.deleted = false) AND (rel_union.r1 = uuids.id))
                  GROUP BY sibs_1.type) rels) sibs ON (true))
     LEFT JOIN LATERAL ( SELECT count(*) AS annotation_count
           FROM public.annotations
          WHERE (annotations.uuids_id = uuids.id)) ac ON (true))
     LEFT JOIN public.data ON (((latest.data_etag)::text = (data.etag)::text)));


ALTER TABLE public.idigbio_uuids_test OWNER TO postgres;

--
-- Name: media; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.media (
    id bigint NOT NULL,
    url text,
    type character varying(20),
    mime character varying(255),
    last_status integer,
    last_check timestamp without time zone,
    owner uuid DEFAULT '872733a2-67a3-4c54-aa76-862735a5f334'::uuid
);


ALTER TABLE public.media OWNER TO idigbio;

--
-- Name: media_id_seq; Type: SEQUENCE; Schema: public; Owner: idigbio
--

CREATE SEQUENCE public.media_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.media_id_seq OWNER TO idigbio;

--
-- Name: media_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: idigbio
--

ALTER SEQUENCE public.media_id_seq OWNED BY public.media.id;


--
-- Name: media_objects; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.media_objects (
    id bigint NOT NULL,
    url text NOT NULL,
    etag character varying(41) NOT NULL,
    modified timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.media_objects OWNER TO idigbio;

--
-- Name: media_objects_id_seq; Type: SEQUENCE; Schema: public; Owner: idigbio
--

CREATE SEQUENCE public.media_objects_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.media_objects_id_seq OWNER TO idigbio;

--
-- Name: media_objects_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: idigbio
--

ALTER SEQUENCE public.media_objects_id_seq OWNED BY public.media_objects.id;


--
-- Name: objects; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.objects (
    id bigint NOT NULL,
    bucket character varying(255) NOT NULL,
    etag character varying(41) NOT NULL,
    detected_mime character varying(255),
    derivatives boolean DEFAULT false
);


ALTER TABLE public.objects OWNER TO idigbio;

--
-- Name: objects_id_seq; Type: SEQUENCE; Schema: public; Owner: idigbio
--

CREATE SEQUENCE public.objects_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.objects_id_seq OWNER TO idigbio;

--
-- Name: objects_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: idigbio
--

ALTER SEQUENCE public.objects_id_seq OWNED BY public.objects.id;


--
-- Name: publishers; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.publishers (
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


ALTER TABLE public.publishers OWNER TO idigbio;

--
-- Name: publishers_id_seq; Type: SEQUENCE; Schema: public; Owner: idigbio
--

CREATE SEQUENCE public.publishers_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.publishers_id_seq OWNER TO idigbio;

--
-- Name: publishers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: idigbio
--

ALTER SEQUENCE public.publishers_id_seq OWNED BY public.publishers.id;


--
-- Name: recordsets; Type: TABLE; Schema: public; Owner: idigbio
--

CREATE TABLE public.recordsets (
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
    eml_harvest_etag character varying(41),
    ingest_is_paused boolean DEFAULT false NOT NULL,
    ingest_paused_date timestamp without time zone
);


ALTER TABLE public.recordsets OWNER TO idigbio;

--
-- Name: recordsets_id_seq; Type: SEQUENCE; Schema: public; Owner: idigbio
--

CREATE SEQUENCE public.recordsets_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.recordsets_id_seq OWNER TO idigbio;

--
-- Name: recordsets_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: idigbio
--

ALTER SEQUENCE public.recordsets_id_seq OWNED BY public.recordsets.id;


--
-- Name: uuids_data_id_seq; Type: SEQUENCE; Schema: public; Owner: idigbio
--

CREATE SEQUENCE public.uuids_data_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.uuids_data_id_seq OWNER TO idigbio;

--
-- Name: uuids_data_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: idigbio
--

ALTER SEQUENCE public.uuids_data_id_seq OWNED BY public.uuids_data.id;


--
-- Name: uuids_identifier_id_seq; Type: SEQUENCE; Schema: public; Owner: idigbio
--

CREATE SEQUENCE public.uuids_identifier_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.uuids_identifier_id_seq OWNER TO idigbio;

--
-- Name: uuids_identifier_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: idigbio
--

ALTER SEQUENCE public.uuids_identifier_id_seq OWNED BY public.uuids_identifier.id;


--
-- Name: uuids_siblings_id_seq; Type: SEQUENCE; Schema: public; Owner: idigbio
--

CREATE SEQUENCE public.uuids_siblings_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.uuids_siblings_id_seq OWNER TO idigbio;

--
-- Name: uuids_siblings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: idigbio
--

ALTER SEQUENCE public.uuids_siblings_id_seq OWNED BY public.uuids_siblings.id;


--
-- Name: annotations id; Type: DEFAULT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.annotations ALTER COLUMN id SET DEFAULT nextval('public.annotations_id_seq'::regclass);


--
-- Name: corrections id; Type: DEFAULT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.corrections ALTER COLUMN id SET DEFAULT nextval('public.corrections_id_seq'::regclass);


--
-- Name: media id; Type: DEFAULT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.media ALTER COLUMN id SET DEFAULT nextval('public.media_id_seq'::regclass);


--
-- Name: media_objects id; Type: DEFAULT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.media_objects ALTER COLUMN id SET DEFAULT nextval('public.media_objects_id_seq'::regclass);


--
-- Name: objects id; Type: DEFAULT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.objects ALTER COLUMN id SET DEFAULT nextval('public.objects_id_seq'::regclass);


--
-- Name: publishers id; Type: DEFAULT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.publishers ALTER COLUMN id SET DEFAULT nextval('public.publishers_id_seq'::regclass);


--
-- Name: recordsets id; Type: DEFAULT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.recordsets ALTER COLUMN id SET DEFAULT nextval('public.recordsets_id_seq'::regclass);


--
-- Name: uuids_data id; Type: DEFAULT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.uuids_data ALTER COLUMN id SET DEFAULT nextval('public.uuids_data_id_seq'::regclass);


--
-- Name: uuids_identifier id; Type: DEFAULT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.uuids_identifier ALTER COLUMN id SET DEFAULT nextval('public.uuids_identifier_id_seq'::regclass);


--
-- Name: uuids_siblings id; Type: DEFAULT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.uuids_siblings ALTER COLUMN id SET DEFAULT nextval('public.uuids_siblings_id_seq'::regclass);


--
-- Name: annotations annotations_pkey; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.annotations
    ADD CONSTRAINT annotations_pkey PRIMARY KEY (id);


--
-- Name: annotations annotations_source_id_key; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.annotations
    ADD CONSTRAINT annotations_source_id_key UNIQUE (source_id);


--
-- Name: corrections corrections_pkey; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.corrections
    ADD CONSTRAINT corrections_pkey PRIMARY KEY (id);


--
-- Name: data data_pkey; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.data
    ADD CONSTRAINT data_pkey PRIMARY KEY (etag);


--
-- Name: idb_object_keys idb_object_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.idb_object_keys
    ADD CONSTRAINT idb_object_keys_pkey PRIMARY KEY (lookup_key);


--
-- Name: recordsets idx_file_link_unique; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.recordsets
    ADD CONSTRAINT idx_file_link_unique UNIQUE (file_link);


--
-- Name: media_objects media_objects_pkey; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.media_objects
    ADD CONSTRAINT media_objects_pkey PRIMARY KEY (id);


--
-- Name: media media_pkey; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.media
    ADD CONSTRAINT media_pkey PRIMARY KEY (id);


--
-- Name: media media_url_key; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.media
    ADD CONSTRAINT media_url_key UNIQUE (url);


--
-- Name: objects objects_etag_key; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.objects
    ADD CONSTRAINT objects_etag_key UNIQUE (etag);


--
-- Name: objects objects_pkey; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.objects
    ADD CONSTRAINT objects_pkey PRIMARY KEY (id);


--
-- Name: publishers publishers_pkey; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.publishers
    ADD CONSTRAINT publishers_pkey PRIMARY KEY (id);


--
-- Name: publishers publishers_uuid_key; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.publishers
    ADD CONSTRAINT publishers_uuid_key UNIQUE (uuid);


--
-- Name: recordsets recordsets_pkey; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.recordsets
    ADD CONSTRAINT recordsets_pkey PRIMARY KEY (id);


--
-- Name: recordsets recordsets_uuid_key; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.recordsets
    ADD CONSTRAINT recordsets_uuid_key UNIQUE (uuid);


--
-- Name: uuids_data uuids_data_pkey; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.uuids_data
    ADD CONSTRAINT uuids_data_pkey PRIMARY KEY (id);


--
-- Name: uuids_identifier uuids_identifier_identifier_key; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.uuids_identifier
    ADD CONSTRAINT uuids_identifier_identifier_key UNIQUE (identifier);


--
-- Name: uuids_identifier uuids_identifier_pkey; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.uuids_identifier
    ADD CONSTRAINT uuids_identifier_pkey PRIMARY KEY (id);


--
-- Name: uuids uuids_pkey; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.uuids
    ADD CONSTRAINT uuids_pkey PRIMARY KEY (id);


--
-- Name: uuids_siblings uuids_siblings_pkey; Type: CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.uuids_siblings
    ADD CONSTRAINT uuids_siblings_pkey PRIMARY KEY (id);


--
-- Name: ceph_objects_bucket_name; Type: INDEX; Schema: public; Owner: idigbio
--

CREATE UNIQUE INDEX ceph_objects_bucket_name ON public.ceph_objects USING btree (ceph_name, ceph_bucket);


--
-- Name: corrections_source; Type: INDEX; Schema: public; Owner: idigbio
--

CREATE INDEX corrections_source ON public.corrections USING btree (source);


--
-- Name: idb_object_keys_etag; Type: INDEX; Schema: public; Owner: idigbio
--

CREATE INDEX idb_object_keys_etag ON public.idb_object_keys USING btree (etag);


--
-- Name: index_ceph_on_filename_with_pattern_ops; Type: INDEX; Schema: public; Owner: idigbio
--

CREATE INDEX index_ceph_on_filename_with_pattern_ops ON public.ceph_server_files USING btree (filename text_pattern_ops);


--
-- Name: media_objects_etags; Type: INDEX; Schema: public; Owner: idigbio
--

CREATE INDEX media_objects_etags ON public.media_objects USING btree (etag);


--
-- Name: media_objects_urls; Type: INDEX; Schema: public; Owner: idigbio
--

CREATE INDEX media_objects_urls ON public.media_objects USING btree (url, modified DESC);


--
-- Name: uuids_data_data_etag_idx; Type: INDEX; Schema: public; Owner: idigbio
--

CREATE INDEX uuids_data_data_etag_idx ON public.uuids_data USING btree (data_etag);


--
-- Name: uuids_data_uuids_id_modified; Type: INDEX; Schema: public; Owner: idigbio
--

CREATE INDEX uuids_data_uuids_id_modified ON public.uuids_data USING btree (uuids_id, modified DESC);


--
-- Name: uuids_data_version; Type: INDEX; Schema: public; Owner: idigbio
--

CREATE INDEX uuids_data_version ON public.uuids_data USING btree (version);


--
-- Name: uuids_identifier_reverse_idx; Type: INDEX; Schema: public; Owner: idigbio
--

CREATE INDEX uuids_identifier_reverse_idx ON public.uuids_identifier USING btree (reverse(identifier) text_pattern_ops);


--
-- Name: uuids_identifier_uuids_id; Type: INDEX; Schema: public; Owner: idigbio
--

CREATE INDEX uuids_identifier_uuids_id ON public.uuids_identifier USING btree (uuids_id);


--
-- Name: uuids_parent; Type: INDEX; Schema: public; Owner: idigbio
--

CREATE INDEX uuids_parent ON public.uuids USING btree (parent);


--
-- Name: uuids_siblings_r1; Type: INDEX; Schema: public; Owner: idigbio
--

CREATE INDEX uuids_siblings_r1 ON public.uuids_siblings USING btree (r1);


--
-- Name: uuids_siblings_r2; Type: INDEX; Schema: public; Owner: idigbio
--

CREATE INDEX uuids_siblings_r2 ON public.uuids_siblings USING btree (r2);


--
-- Name: uuids_type_parent; Type: INDEX; Schema: public; Owner: idigbio
--

CREATE INDEX uuids_type_parent ON public.uuids USING btree (type, parent) WHERE (deleted = false);


--
-- Name: recordsets trigger_set_ingest_paused_date; Type: TRIGGER; Schema: public; Owner: idigbio
--

CREATE TRIGGER trigger_set_ingest_paused_date BEFORE UPDATE ON public.recordsets FOR EACH ROW EXECUTE FUNCTION public.set_ingest_paused_date();


--
-- Name: annotations annotations_uuids_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.annotations
    ADD CONSTRAINT annotations_uuids_id_fkey FOREIGN KEY (uuids_id) REFERENCES public.uuids(id);


--
-- Name: media_objects media_objects_etag_fkey; Type: FK CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.media_objects
    ADD CONSTRAINT media_objects_etag_fkey FOREIGN KEY (etag) REFERENCES public.objects(etag) ON DELETE CASCADE;


--
-- Name: media_objects media_objects_url_fkey; Type: FK CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.media_objects
    ADD CONSTRAINT media_objects_url_fkey FOREIGN KEY (url) REFERENCES public.media(url) ON DELETE CASCADE;


--
-- Name: recordsets recordsets_publisher_uuid_fkey; Type: FK CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.recordsets
    ADD CONSTRAINT recordsets_publisher_uuid_fkey FOREIGN KEY (publisher_uuid) REFERENCES public.publishers(uuid);


--
-- Name: uuids_data uuids_data_data_etag_fkey; Type: FK CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.uuids_data
    ADD CONSTRAINT uuids_data_data_etag_fkey FOREIGN KEY (data_etag) REFERENCES public.data(etag);


--
-- Name: uuids_data uuids_data_uuids_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.uuids_data
    ADD CONSTRAINT uuids_data_uuids_id_fkey FOREIGN KEY (uuids_id) REFERENCES public.uuids(id);


--
-- Name: uuids_identifier uuids_identifier_uuids_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.uuids_identifier
    ADD CONSTRAINT uuids_identifier_uuids_id_fkey FOREIGN KEY (uuids_id) REFERENCES public.uuids(id);


--
-- Name: uuids_siblings uuids_siblings_r1_fkey; Type: FK CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.uuids_siblings
    ADD CONSTRAINT uuids_siblings_r1_fkey FOREIGN KEY (r1) REFERENCES public.uuids(id);


--
-- Name: uuids_siblings uuids_siblings_r2_fkey; Type: FK CONSTRAINT; Schema: public; Owner: idigbio
--

ALTER TABLE ONLY public.uuids_siblings
    ADD CONSTRAINT uuids_siblings_r2_fkey FOREIGN KEY (r2) REFERENCES public.uuids(id);


--
-- Name: TABLE annotations; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.annotations TO idigbio_reader;


--
-- Name: TABLE ceph_objects; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.ceph_objects TO idigbio_reader;


--
-- Name: TABLE ceph_server_files; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.ceph_server_files TO idigbio_reader;


--
-- Name: TABLE corrections; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.corrections TO idigbio_reader;


--
-- Name: TABLE data; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.data TO idigbio_reader;


--
-- Name: TABLE idb_api_keys; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.idb_api_keys TO idigbio_reader;


--
-- Name: TABLE idb_object_keys; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.idb_object_keys TO idigbio_reader;


--
-- Name: TABLE uuids; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.uuids TO idigbio_reader;


--
-- Name: TABLE uuids_siblings; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.uuids_siblings TO idigbio_reader;


--
-- Name: TABLE idigbio_relations; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.idigbio_relations TO idigbio_reader;


--
-- Name: TABLE uuids_data; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.uuids_data TO idigbio_reader;


--
-- Name: TABLE uuids_identifier; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.uuids_identifier TO idigbio_reader;


--
-- Name: TABLE idigbio_uuids_data; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.idigbio_uuids_data TO idigbio_reader;


--
-- Name: TABLE idigbio_uuids_new; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.idigbio_uuids_new TO idigbio_reader;


--
-- Name: TABLE idigbio_uuids_new_ron; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.idigbio_uuids_new_ron TO idigbio_reader;


--
-- Name: TABLE idigbio_uuids_test; Type: ACL; Schema: public; Owner: postgres
--

GRANT SELECT ON TABLE public.idigbio_uuids_test TO idigbio_reader;


--
-- Name: TABLE media; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.media TO idigbio_reader;


--
-- Name: TABLE media_objects; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.media_objects TO idigbio_reader;


--
-- Name: TABLE objects; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.objects TO idigbio_reader;


--
-- Name: TABLE publishers; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.publishers TO idigbio_reader;


--
-- Name: TABLE recordsets; Type: ACL; Schema: public; Owner: idigbio
--

GRANT SELECT ON TABLE public.recordsets TO idigbio_reader;


--
-- Name: DEFAULT PRIVILEGES FOR TABLES; Type: DEFAULT ACL; Schema: public; Owner: postgres
--

ALTER DEFAULT PRIVILEGES FOR ROLE postgres IN SCHEMA public GRANT SELECT ON TABLES  TO idigbio_reader;


--
-- PostgreSQL database dump complete
--

