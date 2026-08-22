--
-- PostgreSQL database dump
--


-- Dumped from database version 16.14
-- Dumped by pg_dump version 16.14

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

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: actor_subscriptions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.actor_subscriptions (
    id integer NOT NULL,
    tmdb_person_id integer NOT NULL,
    actor_name text NOT NULL,
    profile_path text,
    config_start_year integer DEFAULT 1900,
    config_media_types text DEFAULT 'Movie,TV'::text,
    config_genres_include_json jsonb,
    config_genres_exclude_json jsonb,
    status text DEFAULT 'active'::text,
    last_scanned_tmdb_ids_json jsonb,
    last_checked_at timestamp with time zone,
    added_at timestamp with time zone DEFAULT now(),
    config_min_rating real DEFAULT 6.0,
    config_main_role_only boolean DEFAULT false NOT NULL,
    config_min_vote_count integer DEFAULT 10 NOT NULL
);


--
-- Name: actor_subscriptions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.actor_subscriptions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: actor_subscriptions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.actor_subscriptions_id_seq OWNED BY public.actor_subscriptions.id;


--
-- Name: app_settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.app_settings (
    setting_key text NOT NULL,
    value_json jsonb,
    last_updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: cleanup_index; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cleanup_index (
    id integer NOT NULL,
    tmdb_id text NOT NULL,
    item_type text NOT NULL,
    status text DEFAULT 'pending'::text,
    versions_info_json jsonb,
    best_version_json jsonb,
    created_at timestamp with time zone DEFAULT now(),
    last_updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: cleanup_index_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.cleanup_index_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: cleanup_index_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.cleanup_index_id_seq OWNED BY public.cleanup_index.id;


--
-- Name: collections_info; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.collections_info (
    tmdb_collection_id text NOT NULL,
    emby_collection_id text,
    name text,
    overview text,
    last_checked_at timestamp with time zone,
    poster_path text,
    backdrop_path text,
    metadata_schema_version integer DEFAULT 0 NOT NULL,
    is_active boolean DEFAULT false NOT NULL,
    item_type text DEFAULT 'Movie'::text NOT NULL,
    all_tmdb_ids_json jsonb
);


--
-- Name: custom_collections; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.custom_collections (
    id integer NOT NULL,
    name text NOT NULL,
    type text NOT NULL,
    definition_json jsonb NOT NULL,
    status text DEFAULT 'active'::text,
    emby_collection_id text,
    allowed_user_ids jsonb,
    last_synced_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    item_type text,
    in_library_count integer DEFAULT 0,
    generated_media_info_json jsonb,
    sort_order integer DEFAULT 0 NOT NULL
);


--
-- Name: custom_collections_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.custom_collections_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: custom_collections_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.custom_collections_id_seq OWNED BY public.custom_collections.id;


--
-- Name: emby_favorite_items; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.emby_favorite_items (
    user_id text NOT NULL,
    item_id text NOT NULL,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: emby_search_index; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.emby_search_index (
    emby_item_id bigint NOT NULL,
    item_type text NOT NULL,
    title text DEFAULT ''::text NOT NULL,
    original_title text DEFAULT ''::text NOT NULL,
    series_name text DEFAULT ''::text NOT NULL,
    search_compact text DEFAULT ''::text NOT NULL,
    pinyin_full text DEFAULT ''::text NOT NULL,
    pinyin_initials text DEFAULT ''::text NOT NULL,
    search_ngrams text[] DEFAULT '{}'::text[] NOT NULL,
    pinyin_ngrams text[] DEFAULT '{}'::text[] NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: emby_search_rebuild_state; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.emby_search_rebuild_state (
    token text NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: emby_users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.emby_users (
    id text NOT NULL,
    name text NOT NULL,
    is_administrator boolean,
    last_seen_at timestamp with time zone,
    profile_image_tag text,
    policy_json jsonb,
    last_updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: emby_users_extended; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.emby_users_extended (
    emby_user_id text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    registration_date timestamp with time zone DEFAULT now(),
    expiration_date timestamp with time zone,
    notes text,
    created_by text DEFAULT 'self-registered'::text,
    template_id integer,
    telegram_chat_id text
);


--
-- Name: failed_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.failed_log (
    item_id text NOT NULL,
    item_name text,
    reason text,
    failed_at timestamp with time zone DEFAULT now(),
    error_message text,
    item_type text,
    score real
);


--
-- Name: invitations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.invitations (
    id integer NOT NULL,
    token text NOT NULL,
    template_id integer NOT NULL,
    expiration_days integer NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    expires_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    used_by_user_id text
);


--
-- Name: invitations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.invitations_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: invitations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.invitations_id_seq OWNED BY public.invitations.id;


--
-- Name: media_image_cache; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.media_image_cache (
    source_url text NOT NULL,
    content_hash character(64) NOT NULL,
    relative_path text NOT NULL,
    mime_type text NOT NULL,
    byte_size bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    last_accessed_at timestamp with time zone DEFAULT now()
);


--
-- Name: media_image_policy_cache; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.media_image_policy_cache (
    item_type text NOT NULL,
    tmdb_id text NOT NULL,
    season_number integer DEFAULT '-1'::integer NOT NULL,
    episode_number integer DEFAULT '-1'::integer NOT NULL,
    policy_json jsonb DEFAULT '[]'::jsonb NOT NULL,
    images_json jsonb DEFAULT '[]'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: media_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.media_metadata (
    tmdb_id text NOT NULL,
    item_type text NOT NULL,
    in_library boolean DEFAULT false NOT NULL,
    emby_item_ids_json jsonb DEFAULT '[]'::jsonb NOT NULL,
    file_sha1_json jsonb DEFAULT '[]'::jsonb NOT NULL,
    file_pickcode_json jsonb DEFAULT '[]'::jsonb NOT NULL,
    date_added timestamp with time zone,
    asset_details_json jsonb,
    subscription_status text DEFAULT 'NONE'::text NOT NULL,
    subscription_sources_json jsonb DEFAULT '[]'::jsonb NOT NULL,
    first_requested_at timestamp with time zone,
    last_subscribed_at timestamp with time zone,
    imdb_id text,
    title text,
    original_title text,
    original_language text,
    overview text,
    tagline text,
    overview_embedding jsonb,
    release_date date,
    release_year integer,
    last_air_date date,
    poster_path text,
    screenshot_hash character(64),
    backdrop_path text,
    logo_path text,
    thumb_path text,
    homepage text,
    runtime_minutes integer,
    rating real,
    official_rating_json jsonb,
    custom_rating text,
    genres_json jsonb,
    actors_json jsonb,
    directors_json jsonb,
    production_companies_json jsonb,
    networks_json jsonb,
    countries_json jsonb,
    keywords_json jsonb,
    last_updated_at timestamp with time zone DEFAULT now(),
    ignore_reason text,
    tags_json jsonb,
    metadata_ready boolean DEFAULT false NOT NULL,
    actors_ready boolean DEFAULT false NOT NULL,
    metadata_schema_version integer DEFAULT 0 NOT NULL,
    parent_series_tmdb_id text,
    season_number integer,
    episode_number integer,
    watching_status text DEFAULT 'NONE'::text,
    paused_until date,
    force_ended boolean DEFAULT false,
    watchlist_last_checked_at timestamp with time zone,
    watchlist_tmdb_status text,
    watchlist_next_episode_json jsonb,
    watchlist_missing_info_json jsonb,
    last_episode_to_air_json jsonb,
    total_episodes integer DEFAULT 0,
    total_episodes_locked boolean DEFAULT false,
    waiting_for_completed_pack boolean DEFAULT false,
    active_washing boolean DEFAULT false,
    watchlist_version_lock_json jsonb DEFAULT '{}'::jsonb,
    washing_level integer,
    washing_snapshot_json jsonb DEFAULT '{}'::jsonb,
    last_synced_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: p115_filesystem_cache; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.p115_filesystem_cache (
    id text NOT NULL,
    parent_id text NOT NULL,
    name text NOT NULL,
    local_path text,
    sha1 text,
    pick_code text,
    size bigint DEFAULT 0,
    washing_level integer,
    washing_snapshot_json jsonb DEFAULT '{}'::jsonb,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: p115_intro_detection_failures; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.p115_intro_detection_failures (
    failure_key text NOT NULL,
    kind text NOT NULL,
    algorithm_version integer NOT NULL,
    scope text DEFAULT 'season'::text NOT NULL,
    series_tmdb_id text DEFAULT ''::text NOT NULL,
    season_number integer DEFAULT 0 NOT NULL,
    sha1 text DEFAULT ''::text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    sample_count integer DEFAULT 0 NOT NULL,
    episode_count integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: p115_intro_fingerprint_cache; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.p115_intro_fingerprint_cache (
    sha1 text NOT NULL,
    fingerprint_zlib bytea NOT NULL,
    fingerprint_count integer DEFAULT 0 NOT NULL,
    sample_seconds integer DEFAULT 600 NOT NULL,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: p115_intro_fingerprint_failures; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.p115_intro_fingerprint_failures (
    failure_key text NOT NULL,
    kind text NOT NULL,
    algorithm_version integer NOT NULL,
    attempts integer DEFAULT 0 NOT NULL,
    last_sample_seconds integer DEFAULT 0 NOT NULL,
    last_reason text DEFAULT ''::text NOT NULL,
    series_tmdb_id text DEFAULT ''::text NOT NULL,
    season_number integer DEFAULT 0 NOT NULL,
    sha1 text DEFAULT ''::text NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: p115_mediainfo_cache; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.p115_mediainfo_cache (
    sha1 text NOT NULL,
    mediainfo_json jsonb,
    raw_ffprobe_json jsonb,
    created_at timestamp with time zone DEFAULT now(),
    hit_count integer DEFAULT 0
);


--
-- Name: p115_organize_records; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.p115_organize_records (
    id integer NOT NULL,
    file_id text NOT NULL,
    pick_code text,
    original_name text NOT NULL,
    renamed_name text,
    status text NOT NULL,
    fail_reason text,
    tmdb_id text,
    media_type text,
    target_cid text,
    category_name text,
    processed_at timestamp with time zone DEFAULT now(),
    season_number integer
);


--
-- Name: p115_organize_records_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.p115_organize_records_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: p115_organize_records_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.p115_organize_records_id_seq OWNED BY public.p115_organize_records.id;


--
-- Name: p115_upload_records; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.p115_upload_records (
    record_key text NOT NULL,
    record_type text NOT NULL,
    mapping_id text,
    local_path text,
    relative_path text,
    target_cid text,
    pick_code text,
    status text,
    payload_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    CONSTRAINT chk_p115_upload_record_type CHECK ((record_type = ANY (ARRAY['job'::text, 'completed'::text])))
);


--
-- Name: person_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.person_metadata (
    tmdb_person_id integer NOT NULL,
    imdb_id text,
    douban_celebrity_id text,
    primary_name text NOT NULL,
    original_name text,
    profile_path text,
    gender integer,
    adult boolean,
    popularity real,
    last_synced_at timestamp with time zone,
    last_updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: processed_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.processed_log (
    item_id text NOT NULL,
    item_name text,
    processed_at timestamp with time zone DEFAULT now(),
    score real,
    assets_synced_at timestamp with time zone,
    last_emby_modified_at timestamp with time zone
);


--
-- Name: resubscribe_index; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resubscribe_index (
    tmdb_id text NOT NULL,
    item_type text NOT NULL,
    season_number integer DEFAULT '-1'::integer NOT NULL,
    status text NOT NULL,
    reason text,
    matched_rule_id integer,
    last_checked_at timestamp with time zone DEFAULT now()
);


--
-- Name: resubscribe_rules; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resubscribe_rules (
    id integer NOT NULL,
    name text NOT NULL,
    enabled boolean DEFAULT true,
    scope_rules jsonb DEFAULT '[]'::jsonb,
    sort_order integer DEFAULT 0,
    resubscribe_resolution_enabled boolean DEFAULT false,
    resubscribe_resolution_threshold integer DEFAULT 1920,
    resubscribe_audio_enabled boolean DEFAULT false,
    resubscribe_audio_missing_languages jsonb,
    resubscribe_subtitle_enabled boolean DEFAULT false,
    resubscribe_subtitle_missing_languages jsonb,
    resubscribe_quality_enabled boolean DEFAULT false,
    resubscribe_quality_include jsonb,
    resubscribe_effect_enabled boolean DEFAULT false,
    resubscribe_effect_include jsonb,
    resubscribe_subtitle_effect_only boolean DEFAULT false,
    resubscribe_filesize_enabled boolean DEFAULT false,
    resubscribe_filesize_operator text DEFAULT 'lt'::text,
    resubscribe_filesize_threshold_gb real DEFAULT 10.0,
    resubscribe_codec_enabled boolean DEFAULT false,
    resubscribe_codec_include jsonb,
    resubscribe_subtitle_skip_if_audio_exists boolean DEFAULT false,
    custom_resubscribe_enabled boolean DEFAULT false,
    consistency_check_enabled boolean DEFAULT false,
    consistency_must_match_resolution boolean DEFAULT false,
    consistency_must_match_group boolean DEFAULT false,
    consistency_must_match_code boolean DEFAULT false,
    consistency_must_match_codec boolean DEFAULT false,
    rule_type text DEFAULT 'resubscribe'::text,
    filter_rating_enabled boolean DEFAULT false,
    filter_rating_min real DEFAULT 0,
    delete_mode text DEFAULT 'episode'::text,
    delete_delay_seconds integer DEFAULT 0,
    filter_rating_ignore_zero boolean DEFAULT false,
    filter_missing_episodes_enabled boolean DEFAULT false,
    resubscribe_source text DEFAULT 'moviepilot'::text,
    resubscribe_entire_season boolean DEFAULT false
);


--
-- Name: resubscribe_rules_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.resubscribe_rules_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: resubscribe_rules_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.resubscribe_rules_id_seq OWNED BY public.resubscribe_rules.id;


--
-- Name: shared_completed_season_share_channels; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.shared_completed_season_share_channels (
    id integer NOT NULL,
    channel_id text NOT NULL,
    center_source_id text NOT NULL,
    local_source_id integer,
    hub_id text,
    manifest_hash text,
    share_code text,
    receive_code text,
    share_url text,
    share_title text,
    root_fid text,
    root_cid text,
    root_name text,
    file_count integer DEFAULT 0 NOT NULL,
    total_size bigint DEFAULT 0,
    status text DEFAULT 'creating'::text NOT NULL,
    review_status text,
    status_message text,
    fail_count integer DEFAULT 0 NOT NULL,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    last_checked_at timestamp with time zone,
    last_reported_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: shared_completed_season_share_channels_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.shared_completed_season_share_channels_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: shared_completed_season_share_channels_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.shared_completed_season_share_channels_id_seq OWNED BY public.shared_completed_season_share_channels.id;


--
-- Name: shared_credit_ledger_local; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.shared_credit_ledger_local (
    id integer NOT NULL,
    event_type text NOT NULL,
    delta integer DEFAULT 0 NOT NULL,
    reason text,
    ref_id text,
    source_id text,
    virtual_id text,
    tmdb_id text,
    item_type text,
    title text,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: shared_credit_ledger_local_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.shared_credit_ledger_local_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: shared_credit_ledger_local_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.shared_credit_ledger_local_id_seq OWNED BY public.shared_credit_ledger_local.id;


--
-- Name: shared_credit_snapshot; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.shared_credit_snapshot (
    id integer DEFAULT 1 NOT NULL,
    device_id text,
    credit integer DEFAULT 0 NOT NULL,
    contributed_sources integer DEFAULT 0 NOT NULL,
    consumed_sources integer DEFAULT 0 NOT NULL,
    transfer_success integer DEFAULT 0 NOT NULL,
    transfer_failed integer DEFAULT 0 NOT NULL,
    wanted_gaps integer DEFAULT 0 NOT NULL,
    shared_sources integer DEFAULT 0 NOT NULL,
    raw_ffprobe integer DEFAULT 0 NOT NULL,
    remote_devices integer DEFAULT 0 NOT NULL,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now(),
    CONSTRAINT shared_credit_snapshot_singleton CHECK ((id = 1))
);


--
-- Name: shared_rapid_source_files; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.shared_rapid_source_files (
    id integer NOT NULL,
    local_source_id integer NOT NULL,
    fid text,
    pick_code text,
    sha1 text NOT NULL,
    preid text,
    size bigint DEFAULT 0,
    file_name text NOT NULL,
    relative_path text,
    tmdb_id text,
    item_type text,
    season_number integer,
    episode_number integer,
    center_file_id text,
    raw_ffprobe_uploaded boolean DEFAULT false NOT NULL,
    media_signature_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    rapid_meta_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: shared_rapid_source_files_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.shared_rapid_source_files_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: shared_rapid_source_files_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.shared_rapid_source_files_id_seq OWNED BY public.shared_rapid_source_files.id;


--
-- Name: shared_rapid_sources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.shared_rapid_sources (
    id integer NOT NULL,
    source_key text NOT NULL,
    source_kind text NOT NULL,
    center_source_id text,
    tmdb_id text NOT NULL,
    item_type text NOT NULL,
    parent_series_tmdb_id text,
    season_number integer,
    episode_number integer,
    title text,
    release_year integer,
    sha1 text,
    preid text,
    size bigint DEFAULT 0,
    file_name text,
    root_fid text,
    root_name text,
    source_provider text DEFAULT 'local'::text NOT NULL,
    status text DEFAULT 'active'::text NOT NULL,
    center_status text DEFAULT 'local'::text NOT NULL,
    manifest_hash text,
    manifest_version integer DEFAULT 1 NOT NULL,
    file_count integer DEFAULT 0 NOT NULL,
    total_size bigint DEFAULT 0 NOT NULL,
    is_clean_version boolean DEFAULT false NOT NULL,
    clean_version_confidence real,
    clean_version_meta_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    media_signature_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    rapid_meta_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    last_error text,
    reported_at timestamp with time zone,
    disabled_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: shared_rapid_sources_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.shared_rapid_sources_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: shared_rapid_sources_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.shared_rapid_sources_id_seq OWNED BY public.shared_rapid_sources.id;


--
-- Name: shared_virtual_imports; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.shared_virtual_imports (
    id integer NOT NULL,
    source_kind text NOT NULL,
    source_id text NOT NULL,
    tmdb_id text,
    item_type text,
    parent_series_tmdb_id text,
    season_number integer,
    episode_number integer,
    title text,
    release_year integer,
    file_count integer DEFAULT 0 NOT NULL,
    total_size bigint DEFAULT 0 NOT NULL,
    status text DEFAULT 'virtual'::text NOT NULL,
    watched_count integer DEFAULT 0 NOT NULL,
    played_percent real DEFAULT 0 NOT NULL,
    strm_paths_json jsonb DEFAULT '[]'::jsonb NOT NULL,
    source_payload_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    files_json jsonb DEFAULT '[]'::jsonb NOT NULL,
    last_played_at timestamp with time zone,
    promoted_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: shared_virtual_imports_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.shared_virtual_imports_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: shared_virtual_imports_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.shared_virtual_imports_id_seq OWNED BY public.shared_virtual_imports.id;


--
-- Name: subscribe_assistant_delete_records; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.subscribe_assistant_delete_records (
    fingerprint text NOT NULL,
    tmdb_id text,
    season_number integer,
    episode_json jsonb DEFAULT '[]'::jsonb NOT NULL,
    reason text,
    created_at timestamp with time zone DEFAULT now(),
    expires_at timestamp with time zone
);


--
-- Name: subscribe_assistant_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.subscribe_assistant_snapshots (
    id integer NOT NULL,
    tmdb_id text NOT NULL,
    item_type text DEFAULT 'Series'::text NOT NULL,
    season_number integer,
    subscribe_id integer,
    scope_total integer DEFAULT 0,
    scope_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    subscribe_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    last_checked_at timestamp with time zone
);


--
-- Name: subscribe_assistant_snapshots_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.subscribe_assistant_snapshots_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: subscribe_assistant_snapshots_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.subscribe_assistant_snapshots_id_seq OWNED BY public.subscribe_assistant_snapshots.id;


--
-- Name: subscribe_assistant_state; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.subscribe_assistant_state (
    state_key text NOT NULL,
    state_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: translation_cache; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.translation_cache (
    original_text text NOT NULL,
    translated_text text,
    engine_used text,
    last_updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: user_media_data; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_media_data (
    user_id text NOT NULL,
    item_id text NOT NULL,
    is_favorite boolean DEFAULT false,
    played boolean DEFAULT false,
    playback_position_ticks bigint DEFAULT 0,
    play_count integer DEFAULT 0,
    last_played_date timestamp with time zone,
    last_updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: user_templates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_templates (
    id integer NOT NULL,
    name text NOT NULL,
    description text,
    emby_policy_json jsonb NOT NULL,
    default_expiration_days integer DEFAULT 30,
    created_at timestamp with time zone DEFAULT now(),
    allow_unrestricted_subscriptions boolean DEFAULT false NOT NULL,
    personal_play_account_only boolean DEFAULT false NOT NULL,
    max_concurrent_streams integer DEFAULT 0 NOT NULL,
    source_emby_user_id text,
    emby_configuration_json jsonb
);


--
-- Name: user_templates_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.user_templates_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: user_templates_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.user_templates_id_seq OWNED BY public.user_templates.id;


--
-- Name: washing_priority_groups; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.washing_priority_groups (
    id integer NOT NULL,
    name text NOT NULL,
    media_type text NOT NULL,
    target_cids jsonb DEFAULT '[]'::jsonb,
    priorities jsonb DEFAULT '[]'::jsonb,
    sort_order integer DEFAULT 0
);


--
-- Name: washing_priority_groups_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.washing_priority_groups_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: washing_priority_groups_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.washing_priority_groups_id_seq OWNED BY public.washing_priority_groups.id;


--
-- Name: actor_subscriptions id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.actor_subscriptions ALTER COLUMN id SET DEFAULT nextval('public.actor_subscriptions_id_seq'::regclass);


--
-- Name: cleanup_index id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cleanup_index ALTER COLUMN id SET DEFAULT nextval('public.cleanup_index_id_seq'::regclass);


--
-- Name: custom_collections id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.custom_collections ALTER COLUMN id SET DEFAULT nextval('public.custom_collections_id_seq'::regclass);


--
-- Name: invitations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.invitations ALTER COLUMN id SET DEFAULT nextval('public.invitations_id_seq'::regclass);


--
-- Name: p115_organize_records id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.p115_organize_records ALTER COLUMN id SET DEFAULT nextval('public.p115_organize_records_id_seq'::regclass);


--
-- Name: resubscribe_rules id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resubscribe_rules ALTER COLUMN id SET DEFAULT nextval('public.resubscribe_rules_id_seq'::regclass);


--
-- Name: shared_completed_season_share_channels id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shared_completed_season_share_channels ALTER COLUMN id SET DEFAULT nextval('public.shared_completed_season_share_channels_id_seq'::regclass);


--
-- Name: shared_credit_ledger_local id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shared_credit_ledger_local ALTER COLUMN id SET DEFAULT nextval('public.shared_credit_ledger_local_id_seq'::regclass);


--
-- Name: shared_rapid_source_files id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shared_rapid_source_files ALTER COLUMN id SET DEFAULT nextval('public.shared_rapid_source_files_id_seq'::regclass);


--
-- Name: shared_rapid_sources id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shared_rapid_sources ALTER COLUMN id SET DEFAULT nextval('public.shared_rapid_sources_id_seq'::regclass);


--
-- Name: shared_virtual_imports id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shared_virtual_imports ALTER COLUMN id SET DEFAULT nextval('public.shared_virtual_imports_id_seq'::regclass);


--
-- Name: subscribe_assistant_snapshots id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.subscribe_assistant_snapshots ALTER COLUMN id SET DEFAULT nextval('public.subscribe_assistant_snapshots_id_seq'::regclass);


--
-- Name: user_templates id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_templates ALTER COLUMN id SET DEFAULT nextval('public.user_templates_id_seq'::regclass);


--
-- Name: washing_priority_groups id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.washing_priority_groups ALTER COLUMN id SET DEFAULT nextval('public.washing_priority_groups_id_seq'::regclass);


--
-- Name: actor_subscriptions actor_subscriptions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.actor_subscriptions
    ADD CONSTRAINT actor_subscriptions_pkey PRIMARY KEY (id);


--
-- Name: actor_subscriptions actor_subscriptions_tmdb_person_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.actor_subscriptions
    ADD CONSTRAINT actor_subscriptions_tmdb_person_id_key UNIQUE (tmdb_person_id);


--
-- Name: app_settings app_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.app_settings
    ADD CONSTRAINT app_settings_pkey PRIMARY KEY (setting_key);


--
-- Name: cleanup_index cleanup_index_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cleanup_index
    ADD CONSTRAINT cleanup_index_pkey PRIMARY KEY (id);


--
-- Name: cleanup_index cleanup_index_tmdb_id_item_type_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cleanup_index
    ADD CONSTRAINT cleanup_index_tmdb_id_item_type_key UNIQUE (tmdb_id, item_type);


--
-- Name: collections_info collections_info_emby_collection_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collections_info
    ADD CONSTRAINT collections_info_emby_collection_id_key UNIQUE (emby_collection_id);


--
-- Name: collections_info collections_info_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.collections_info
    ADD CONSTRAINT collections_info_pkey PRIMARY KEY (tmdb_collection_id);


--
-- Name: custom_collections custom_collections_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.custom_collections
    ADD CONSTRAINT custom_collections_name_key UNIQUE (name);


--
-- Name: custom_collections custom_collections_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.custom_collections
    ADD CONSTRAINT custom_collections_pkey PRIMARY KEY (id);


--
-- Name: emby_favorite_items emby_favorite_items_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.emby_favorite_items
    ADD CONSTRAINT emby_favorite_items_pkey PRIMARY KEY (user_id, item_id);


--
-- Name: emby_search_index emby_search_index_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.emby_search_index
    ADD CONSTRAINT emby_search_index_pkey PRIMARY KEY (emby_item_id);


--
-- Name: emby_search_rebuild_state emby_search_rebuild_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.emby_search_rebuild_state
    ADD CONSTRAINT emby_search_rebuild_state_pkey PRIMARY KEY (token);


--
-- Name: emby_users_extended emby_users_extended_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.emby_users_extended
    ADD CONSTRAINT emby_users_extended_pkey PRIMARY KEY (emby_user_id);


--
-- Name: emby_users emby_users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.emby_users
    ADD CONSTRAINT emby_users_pkey PRIMARY KEY (id);


--
-- Name: failed_log failed_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.failed_log
    ADD CONSTRAINT failed_log_pkey PRIMARY KEY (item_id);


--
-- Name: invitations invitations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.invitations
    ADD CONSTRAINT invitations_pkey PRIMARY KEY (id);


--
-- Name: invitations invitations_token_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.invitations
    ADD CONSTRAINT invitations_token_key UNIQUE (token);


--
-- Name: media_image_cache media_image_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.media_image_cache
    ADD CONSTRAINT media_image_cache_pkey PRIMARY KEY (source_url);


--
-- Name: media_image_policy_cache media_image_policy_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.media_image_policy_cache
    ADD CONSTRAINT media_image_policy_cache_pkey PRIMARY KEY (item_type, tmdb_id, season_number, episode_number);


--
-- Name: media_metadata media_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.media_metadata
    ADD CONSTRAINT media_metadata_pkey PRIMARY KEY (tmdb_id, item_type);


--
-- Name: p115_filesystem_cache p115_filesystem_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.p115_filesystem_cache
    ADD CONSTRAINT p115_filesystem_cache_pkey PRIMARY KEY (id);


--
-- Name: p115_intro_detection_failures p115_intro_detection_failures_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.p115_intro_detection_failures
    ADD CONSTRAINT p115_intro_detection_failures_pkey PRIMARY KEY (failure_key, kind, algorithm_version);


--
-- Name: p115_intro_fingerprint_cache p115_intro_fingerprint_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.p115_intro_fingerprint_cache
    ADD CONSTRAINT p115_intro_fingerprint_cache_pkey PRIMARY KEY (sha1);


--
-- Name: p115_intro_fingerprint_failures p115_intro_fingerprint_failures_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.p115_intro_fingerprint_failures
    ADD CONSTRAINT p115_intro_fingerprint_failures_pkey PRIMARY KEY (failure_key, kind, algorithm_version);


--
-- Name: p115_mediainfo_cache p115_mediainfo_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.p115_mediainfo_cache
    ADD CONSTRAINT p115_mediainfo_cache_pkey PRIMARY KEY (sha1);


--
-- Name: p115_organize_records p115_organize_records_file_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.p115_organize_records
    ADD CONSTRAINT p115_organize_records_file_id_key UNIQUE (file_id);


--
-- Name: p115_organize_records p115_organize_records_pick_code_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.p115_organize_records
    ADD CONSTRAINT p115_organize_records_pick_code_key UNIQUE (pick_code);


--
-- Name: p115_organize_records p115_organize_records_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.p115_organize_records
    ADD CONSTRAINT p115_organize_records_pkey PRIMARY KEY (id);


--
-- Name: p115_upload_records p115_upload_records_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.p115_upload_records
    ADD CONSTRAINT p115_upload_records_pkey PRIMARY KEY (record_key);


--
-- Name: person_metadata person_metadata_douban_celebrity_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.person_metadata
    ADD CONSTRAINT person_metadata_douban_celebrity_id_key UNIQUE (douban_celebrity_id);


--
-- Name: person_metadata person_metadata_imdb_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.person_metadata
    ADD CONSTRAINT person_metadata_imdb_id_key UNIQUE (imdb_id);


--
-- Name: person_metadata person_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.person_metadata
    ADD CONSTRAINT person_metadata_pkey PRIMARY KEY (tmdb_person_id);


--
-- Name: processed_log processed_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.processed_log
    ADD CONSTRAINT processed_log_pkey PRIMARY KEY (item_id);


--
-- Name: resubscribe_index resubscribe_index_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resubscribe_index
    ADD CONSTRAINT resubscribe_index_pkey PRIMARY KEY (tmdb_id, item_type, season_number);


--
-- Name: resubscribe_rules resubscribe_rules_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resubscribe_rules
    ADD CONSTRAINT resubscribe_rules_name_key UNIQUE (name);


--
-- Name: resubscribe_rules resubscribe_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resubscribe_rules
    ADD CONSTRAINT resubscribe_rules_pkey PRIMARY KEY (id);


--
-- Name: shared_completed_season_share_channels shared_completed_season_share_channels_channel_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shared_completed_season_share_channels
    ADD CONSTRAINT shared_completed_season_share_channels_channel_id_key UNIQUE (channel_id);


--
-- Name: shared_completed_season_share_channels shared_completed_season_share_channels_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shared_completed_season_share_channels
    ADD CONSTRAINT shared_completed_season_share_channels_pkey PRIMARY KEY (id);


--
-- Name: shared_credit_ledger_local shared_credit_ledger_local_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shared_credit_ledger_local
    ADD CONSTRAINT shared_credit_ledger_local_pkey PRIMARY KEY (id);


--
-- Name: shared_credit_snapshot shared_credit_snapshot_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shared_credit_snapshot
    ADD CONSTRAINT shared_credit_snapshot_pkey PRIMARY KEY (id);


--
-- Name: shared_rapid_source_files shared_rapid_source_files_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shared_rapid_source_files
    ADD CONSTRAINT shared_rapid_source_files_pkey PRIMARY KEY (id);


--
-- Name: shared_rapid_sources shared_rapid_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shared_rapid_sources
    ADD CONSTRAINT shared_rapid_sources_pkey PRIMARY KEY (id);


--
-- Name: shared_rapid_sources shared_rapid_sources_source_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shared_rapid_sources
    ADD CONSTRAINT shared_rapid_sources_source_key_key UNIQUE (source_key);


--
-- Name: shared_virtual_imports shared_virtual_imports_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shared_virtual_imports
    ADD CONSTRAINT shared_virtual_imports_pkey PRIMARY KEY (id);


--
-- Name: subscribe_assistant_delete_records subscribe_assistant_delete_records_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.subscribe_assistant_delete_records
    ADD CONSTRAINT subscribe_assistant_delete_records_pkey PRIMARY KEY (fingerprint);


--
-- Name: subscribe_assistant_snapshots subscribe_assistant_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.subscribe_assistant_snapshots
    ADD CONSTRAINT subscribe_assistant_snapshots_pkey PRIMARY KEY (id);


--
-- Name: subscribe_assistant_state subscribe_assistant_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.subscribe_assistant_state
    ADD CONSTRAINT subscribe_assistant_state_pkey PRIMARY KEY (state_key);


--
-- Name: translation_cache translation_cache_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.translation_cache
    ADD CONSTRAINT translation_cache_pkey PRIMARY KEY (original_text);


--
-- Name: p115_filesystem_cache uniq_p115_parent_name; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.p115_filesystem_cache
    ADD CONSTRAINT uniq_p115_parent_name UNIQUE (parent_id, name);


--
-- Name: user_media_data user_media_data_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_media_data
    ADD CONSTRAINT user_media_data_pkey PRIMARY KEY (user_id, item_id);


--
-- Name: user_templates user_templates_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_templates
    ADD CONSTRAINT user_templates_name_key UNIQUE (name);


--
-- Name: user_templates user_templates_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_templates
    ADD CONSTRAINT user_templates_pkey PRIMARY KEY (id);


--
-- Name: washing_priority_groups washing_priority_groups_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.washing_priority_groups
    ADD CONSTRAINT washing_priority_groups_pkey PRIMARY KEY (id);


--
-- Name: idx_emby_favorite_items_item; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_emby_favorite_items_item ON public.emby_favorite_items USING btree (item_id);


--
-- Name: idx_emby_search_ngrams; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_emby_search_ngrams ON public.emby_search_index USING gin (search_ngrams);


--
-- Name: idx_emby_search_pinyin_ngrams; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_emby_search_pinyin_ngrams ON public.emby_search_index USING gin (pinyin_ngrams);


--
-- Name: idx_emby_search_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_emby_search_type ON public.emby_search_index USING btree (item_type);


--
-- Name: idx_media_image_cache_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_media_image_cache_hash ON public.media_image_cache USING btree (content_hash);


--
-- Name: idx_mm_actors_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_actors_gin ON public.media_metadata USING gin (actors_json jsonb_path_ops);


--
-- Name: idx_mm_asset_details_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_asset_details_gin ON public.media_metadata USING gin (asset_details_json);


--
-- Name: idx_mm_companies_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_companies_gin ON public.media_metadata USING gin (production_companies_json jsonb_path_ops);


--
-- Name: idx_mm_countries_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_countries_gin ON public.media_metadata USING gin (countries_json jsonb_path_ops);


--
-- Name: idx_mm_date_added_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_date_added_desc ON public.media_metadata USING btree (date_added DESC);


--
-- Name: idx_mm_directors_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_directors_gin ON public.media_metadata USING gin (directors_json jsonb_path_ops);


--
-- Name: idx_mm_emby_ids_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_emby_ids_gin ON public.media_metadata USING gin (emby_item_ids_json);


--
-- Name: idx_mm_episode_parent_library; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_episode_parent_library ON public.media_metadata USING btree (parent_series_tmdb_id, item_type, in_library) WHERE (item_type = 'Episode'::text);


--
-- Name: idx_mm_episode_parent_season_ep_library; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_episode_parent_season_ep_library ON public.media_metadata USING btree (parent_series_tmdb_id, season_number, episode_number) WHERE ((item_type = 'Episode'::text) AND (in_library = true));


--
-- Name: idx_mm_file_pickcode_json_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_file_pickcode_json_gin ON public.media_metadata USING gin (file_pickcode_json);


--
-- Name: idx_mm_file_sha1_json_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_file_sha1_json_gin ON public.media_metadata USING gin (file_sha1_json);


--
-- Name: idx_mm_genres_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_genres_gin ON public.media_metadata USING gin (genres_json jsonb_path_ops);


--
-- Name: idx_mm_in_library; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_in_library ON public.media_metadata USING btree (in_library);


--
-- Name: idx_mm_item_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_item_type ON public.media_metadata USING btree (item_type);


--
-- Name: idx_mm_keywords_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_keywords_gin ON public.media_metadata USING gin (keywords_json jsonb_path_ops);


--
-- Name: idx_mm_networks_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_networks_gin ON public.media_metadata USING gin (networks_json jsonb_path_ops);


--
-- Name: idx_mm_parent_series; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_parent_series ON public.media_metadata USING btree (parent_series_tmdb_id);


--
-- Name: idx_mm_parent_series_season; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_parent_series_season ON public.media_metadata USING btree (parent_series_tmdb_id, season_number);


--
-- Name: idx_mm_rating_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_rating_desc ON public.media_metadata USING btree (rating DESC);


--
-- Name: idx_mm_release_date_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_release_date_desc ON public.media_metadata USING btree (release_date DESC);


--
-- Name: idx_mm_release_year; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_release_year ON public.media_metadata USING btree (release_year);


--
-- Name: idx_mm_screenshot_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_screenshot_hash ON public.media_metadata USING btree (screenshot_hash) WHERE (screenshot_hash IS NOT NULL);


--
-- Name: idx_mm_season_parent_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_season_parent_status ON public.media_metadata USING btree (parent_series_tmdb_id, season_number, watching_status) WHERE (item_type = 'Season'::text);


--
-- Name: idx_mm_subscription_sources_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_subscription_sources_gin ON public.media_metadata USING gin (subscription_sources_json);


--
-- Name: idx_mm_subscription_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_subscription_status ON public.media_metadata USING btree (subscription_status) WHERE (in_library = false);


--
-- Name: idx_mm_tags_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_tags_gin ON public.media_metadata USING gin (tags_json jsonb_path_ops);


--
-- Name: idx_mm_type_parent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_type_parent ON public.media_metadata USING btree (item_type, parent_series_tmdb_id);


--
-- Name: idx_mm_washing_level; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_washing_level ON public.media_metadata USING btree (washing_level) WHERE ((washing_level IS NOT NULL) AND (in_library = true));


--
-- Name: idx_mm_watching_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mm_watching_status ON public.media_metadata USING btree (watching_status) WHERE (watching_status <> 'NONE'::text);


--
-- Name: idx_p115_intro_detection_failures_kind; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_p115_intro_detection_failures_kind ON public.p115_intro_detection_failures USING btree (kind, algorithm_version);


--
-- Name: idx_p115_intro_fingerprint_failures_kind; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_p115_intro_fingerprint_failures_kind ON public.p115_intro_fingerprint_failures USING btree (kind, algorithm_version);


--
-- Name: idx_p115_local_path; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_p115_local_path ON public.p115_filesystem_cache USING btree (local_path) WHERE ((local_path IS NOT NULL) AND (local_path <> ''::text));


--
-- Name: idx_p115_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_p115_name ON public.p115_filesystem_cache USING btree (name);


--
-- Name: idx_p115_parent_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_p115_parent_id ON public.p115_filesystem_cache USING btree (parent_id);


--
-- Name: idx_p115_pick_code; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_p115_pick_code ON public.p115_filesystem_cache USING btree (pick_code) WHERE ((pick_code IS NOT NULL) AND (pick_code <> ''::text));


--
-- Name: idx_p115_sha1_upper; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_p115_sha1_upper ON public.p115_filesystem_cache USING btree (upper(sha1)) WHERE ((sha1 IS NOT NULL) AND (sha1 <> ''::text));


--
-- Name: idx_p115_upload_records_local_path; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_p115_upload_records_local_path ON public.p115_upload_records USING btree (local_path) WHERE (record_type = 'completed'::text);


--
-- Name: idx_p115_upload_records_mapping; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_p115_upload_records_mapping ON public.p115_upload_records USING btree (mapping_id, record_type);


--
-- Name: idx_p115_upload_records_pick_code; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_p115_upload_records_pick_code ON public.p115_upload_records USING btree (pick_code) WHERE (pick_code IS NOT NULL);


--
-- Name: idx_p115_upload_records_type_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_p115_upload_records_type_status ON public.p115_upload_records USING btree (record_type, status);


--
-- Name: idx_p115_washing_level; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_p115_washing_level ON public.p115_filesystem_cache USING btree (washing_level) WHERE (washing_level IS NOT NULL);


--
-- Name: idx_ri_tmdb_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ri_tmdb_type ON public.resubscribe_index USING btree (tmdb_id, item_type);


--
-- Name: idx_sas_delete_records_expires; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sas_delete_records_expires ON public.subscribe_assistant_delete_records USING btree (expires_at);


--
-- Name: idx_sas_snapshots_checked; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sas_snapshots_checked ON public.subscribe_assistant_snapshots USING btree (last_checked_at);


--
-- Name: idx_sas_snapshots_media; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_sas_snapshots_media ON public.subscribe_assistant_snapshots USING btree (tmdb_id, item_type, season_number);


--
-- Name: idx_scssc_center_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scssc_center_source ON public.shared_completed_season_share_channels USING btree (center_source_id, status, updated_at DESC);


--
-- Name: idx_scssc_channel; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scssc_channel ON public.shared_completed_season_share_channels USING btree (channel_id);


--
-- Name: idx_scssc_share_code; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scssc_share_code ON public.shared_completed_season_share_channels USING btree (share_code) WHERE ((share_code IS NOT NULL) AND (share_code <> ''::text));


--
-- Name: idx_scssc_status_check; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_scssc_status_check ON public.shared_completed_season_share_channels USING btree (status, last_checked_at NULLS FIRST, updated_at);


--
-- Name: idx_shared_credit_ledger_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_shared_credit_ledger_created ON public.shared_credit_ledger_local USING btree (created_at DESC);


--
-- Name: idx_srs_center_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_srs_center_source ON public.shared_rapid_sources USING btree (source_kind, center_source_id);


--
-- Name: idx_srs_effective_share_scan; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_srs_effective_share_scan ON public.shared_rapid_sources USING btree (source_kind, tmdb_id, season_number, episode_number, status, center_status) WHERE ((status = ANY (ARRAY['active'::text, 'available'::text, 'updating'::text])) AND (center_status <> 'disabled'::text));


--
-- Name: idx_srs_media; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_srs_media ON public.shared_rapid_sources USING btree (tmdb_id, item_type, season_number, episode_number);


--
-- Name: idx_srs_sha1; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_srs_sha1 ON public.shared_rapid_sources USING btree (upper(sha1)) WHERE ((sha1 IS NOT NULL) AND (sha1 <> ''::text));


--
-- Name: idx_srs_status_center_updated; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_srs_status_center_updated ON public.shared_rapid_sources USING btree (status, center_status, updated_at, id);


--
-- Name: idx_srs_status_updated; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_srs_status_updated ON public.shared_rapid_sources USING btree (status, updated_at DESC);


--
-- Name: idx_srsf_episode; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_srsf_episode ON public.shared_rapid_source_files USING btree (tmdb_id, season_number, episode_number);


--
-- Name: idx_srsf_sha1; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_srsf_sha1 ON public.shared_rapid_source_files USING btree (upper(sha1));


--
-- Name: idx_srsf_source; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_srsf_source ON public.shared_rapid_source_files USING btree (local_source_id);


--
-- Name: idx_srsf_source_episode_sha1; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_srsf_source_episode_sha1 ON public.shared_rapid_source_files USING btree (local_source_id, tmdb_id, season_number, episode_number, upper(sha1));


--
-- Name: idx_svi_media; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_svi_media ON public.shared_virtual_imports USING btree (tmdb_id, item_type, season_number, episode_number);


--
-- Name: idx_svi_status_updated; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_svi_status_updated ON public.shared_virtual_imports USING btree (status, updated_at DESC);


--
-- Name: emby_users_extended emby_users_extended_emby_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.emby_users_extended
    ADD CONSTRAINT emby_users_extended_emby_user_id_fkey FOREIGN KEY (emby_user_id) REFERENCES public.emby_users(id) ON DELETE CASCADE;


--
-- Name: emby_users_extended emby_users_extended_template_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.emby_users_extended
    ADD CONSTRAINT emby_users_extended_template_id_fkey FOREIGN KEY (template_id) REFERENCES public.user_templates(id) ON DELETE SET NULL;


--
-- Name: invitations invitations_template_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.invitations
    ADD CONSTRAINT invitations_template_id_fkey FOREIGN KEY (template_id) REFERENCES public.user_templates(id) ON DELETE CASCADE;


--
-- Name: shared_completed_season_share_channels shared_completed_season_share_channels_local_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shared_completed_season_share_channels
    ADD CONSTRAINT shared_completed_season_share_channels_local_source_id_fkey FOREIGN KEY (local_source_id) REFERENCES public.shared_rapid_sources(id) ON DELETE SET NULL;


--
-- Name: shared_rapid_source_files shared_rapid_source_files_local_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.shared_rapid_source_files
    ADD CONSTRAINT shared_rapid_source_files_local_source_id_fkey FOREIGN KEY (local_source_id) REFERENCES public.shared_rapid_sources(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--
