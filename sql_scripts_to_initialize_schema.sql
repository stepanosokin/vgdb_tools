CREATE SCHEMA IF NOT EXISTS rfgf AUTHORIZATION vgdb;

CREATE TABLE IF NOT EXISTS rfgf.license_blocks_rfgf
(
    id SERIAL PRIMARY KEY,
    geom geometry(MultiPolygon,4326),
    fid bigint,
    license_block_id integer,
    gos_reg_num character varying COLLATE pg_catalog."default",
    date_register date,
    license_purpose character varying COLLATE pg_catalog."default",
    resource_type character varying COLLATE pg_catalog."default",
    license_block_name character varying COLLATE pg_catalog."default",
    region character varying COLLATE pg_catalog."default",
    status character varying COLLATE pg_catalog."default",
    user_info character varying COLLATE pg_catalog."default",
    licensor character varying COLLATE pg_catalog."default",
    license_doc_requisites character varying COLLATE pg_catalog."default",
    license_cancel_order_info character varying COLLATE pg_catalog."default",
    date_stop_subsoil_usage date,
    limit_conditions_stop_subsoil_usage character varying COLLATE pg_catalog."default",
    date_license_stop date,
    previous_license_info character varying COLLATE pg_catalog."default",
    asln_link character varying COLLATE pg_catalog."default",
    source character varying COLLATE pg_catalog."default",
    license_update_info character varying COLLATE pg_catalog."default",
    license_re_registration_info character varying COLLATE pg_catalog."default",
    rfgf_link character varying COLLATE pg_catalog."default",
    comments character varying COLLATE pg_catalog."default",
    source_gcs character varying COLLATE pg_catalog."default",
    coords_text character varying COLLATE pg_catalog."default"
);

CREATE OR REPLACE VIEW rfgf.license_blocks_rfgf_hc_active
 AS
 SELECT row_number() OVER (ORDER BY license_blocks_rfgf.id) AS gid,
    license_blocks_rfgf.id,
    license_blocks_rfgf.geom,
    license_blocks_rfgf.fid,
    license_blocks_rfgf.license_block_id,
    license_blocks_rfgf.gos_reg_num,
    license_blocks_rfgf.date_register,
    license_blocks_rfgf.license_purpose,
    license_blocks_rfgf.resource_type,
    license_blocks_rfgf.license_block_name,
    license_blocks_rfgf.region,
    license_blocks_rfgf.status,
    license_blocks_rfgf.user_info,
    license_blocks_rfgf.licensor,
    license_blocks_rfgf.license_doc_requisites,
    license_blocks_rfgf.license_cancel_order_info,
    license_blocks_rfgf.date_stop_subsoil_usage,
    license_blocks_rfgf.limit_conditions_stop_subsoil_usage,
    license_blocks_rfgf.date_license_stop,
    license_blocks_rfgf.previous_license_info,
    license_blocks_rfgf.asln_link,
    license_blocks_rfgf.source,
    license_blocks_rfgf.license_update_info,
    license_blocks_rfgf.license_re_registration_info,
    license_blocks_rfgf.rfgf_link,
    license_blocks_rfgf.comments,
    license_blocks_rfgf.source_gcs,
    license_blocks_rfgf.coords_text
   FROM rfgf.license_blocks_rfgf
  WHERE (license_blocks_rfgf.resource_type::text = ANY (ARRAY['УВС'::text, 'Углеводородное сырье'::text])) AND license_blocks_rfgf.license_cancel_order_info IS NULL AND (license_blocks_rfgf.date_license_stop IS NULL OR license_blocks_rfgf.date_license_stop >= CURRENT_DATE);


  CREATE OR REPLACE VIEW rfgf.license_blocks_rfgf_hc_canceled
 AS
 SELECT row_number() OVER (ORDER BY license_blocks_rfgf.id) AS gid,
    license_blocks_rfgf.id,
    license_blocks_rfgf.geom,
    license_blocks_rfgf.fid,
    license_blocks_rfgf.license_block_id,
    license_blocks_rfgf.gos_reg_num,
    license_blocks_rfgf.date_register,
    license_blocks_rfgf.license_purpose,
    license_blocks_rfgf.resource_type,
    license_blocks_rfgf.license_block_name,
    license_blocks_rfgf.region,
    license_blocks_rfgf.status,
    license_blocks_rfgf.user_info,
    license_blocks_rfgf.licensor,
    license_blocks_rfgf.license_doc_requisites,
    license_blocks_rfgf.license_cancel_order_info,
    license_blocks_rfgf.date_stop_subsoil_usage,
    license_blocks_rfgf.limit_conditions_stop_subsoil_usage,
    license_blocks_rfgf.date_license_stop,
    license_blocks_rfgf.previous_license_info,
    license_blocks_rfgf.asln_link,
    license_blocks_rfgf.source,
    license_blocks_rfgf.license_update_info,
    license_blocks_rfgf.license_re_registration_info,
    license_blocks_rfgf.rfgf_link,
    license_blocks_rfgf.comments,
    license_blocks_rfgf.source_gcs,
    license_blocks_rfgf.coords_text
   FROM rfgf.license_blocks_rfgf
  WHERE (license_blocks_rfgf.resource_type::text = ANY (ARRAY['УВС'::text, 'Углеводородное сырье'::text])) AND (license_blocks_rfgf.license_cancel_order_info IS NOT NULL OR license_blocks_rfgf.date_license_stop < CURRENT_DATE);

CREATE SCHEMA IF NOT EXISTS torgi_gov_ru AUTHORIZATION vgdb;

CREATE TABLE IF NOT EXISTS torgi_gov_ru.lotcards
(
    gid SERIAL PRIMARY KEY,
    id character varying COLLATE pg_catalog."default",
    "noticeNumber" character varying COLLATE pg_catalog."default",
    "lotNumber" character varying COLLATE pg_catalog."default",
    "lotStatus" character varying COLLATE pg_catalog."default",
    "biddType_code" character varying COLLATE pg_catalog."default",
    "biddType_name" character varying COLLATE pg_catalog."default",
    "biddForm_code" character varying COLLATE pg_catalog."default",
    "biddForm_name" character varying COLLATE pg_catalog."default",
    "lotDescription" character varying COLLATE pg_catalog."default",
    "priceMin" numeric,
    "priceFin" numeric,
    "biddEndTime" timestamp without time zone,
    "mineralResource" character varying COLLATE pg_catalog."default",
    "resourceCategory" character varying COLLATE pg_catalog."default",
    "squareMR" numeric,
    "resourcePotential" character varying COLLATE pg_catalog."default",
    "resourceAreaId" bigint,
    "currencyCode" character varying COLLATE pg_catalog."default",
    "etpCode" character varying COLLATE pg_catalog."default",
    "createDate" timestamp without time zone,
    "timeZoneName" character varying COLLATE pg_catalog."default",
    "timeZoneOffset" character varying COLLATE pg_catalog."default",
    "hasAppeals" boolean,
    "isStopped" boolean,
    "isAnnuled" boolean,
    "miningSiteName_EA(N)" character varying COLLATE pg_catalog."default",
    "resourceTypeUse_EA(N)" character varying COLLATE pg_catalog."default",
    "resourceLocation_EA(N)" character varying COLLATE pg_catalog."default",
    "conditionsOfUse_EA(N)" character varying COLLATE pg_catalog."default",
    "licensePeriod_EA(N)" character varying COLLATE pg_catalog."default",
    "licenseFeeAmount_EA(N)" numeric,
    "licenseProcedureMaking_EA(N)" character varying COLLATE pg_catalog."default",
    "participationFee_EA(N)" numeric,
    "feeProcedureMaking_EA(N)" character varying COLLATE pg_catalog."default",
    "oneTimePaymentProcedure_EA(N)" character varying COLLATE pg_catalog."default",
    "depositTimeAndRules_EA(N)" character varying COLLATE pg_catalog."default",
    "depositRefund_EA(N)" character varying COLLATE pg_catalog."default",
    lot_data json,
    "lotName" character varying COLLATE pg_catalog."default",
    created_user character varying COLLATE pg_catalog."default",
    date_created date,
    last_edited_user character varying COLLATE pg_catalog."default",
    date_edited date,
    rn_guid character varying COLLATE pg_catalog."default",
    rfgf_gos_reg_num character varying COLLATE pg_catalog."default",
    "auctionStartDate" timestamp without time zone
)


CREATE SCHEMA IF NOT EXISTS rosnedra AUTHORIZATION vgdb;

CREATE TABLE IF NOT EXISTS rosnedra.license_blocks_rosnedra_orders
(
    gid SERIAL PRIMARY KEY,
    resource_type character varying COLLATE pg_catalog."default",
    name character varying COLLATE pg_catalog."default",
    area_km double precision,
    reserves_predicted_resources character varying COLLATE pg_catalog."default",
    exp_protocol character varying COLLATE pg_catalog."default",
    usage_type character varying COLLATE pg_catalog."default",
    lend_type character varying COLLATE pg_catalog."default",
    planned_terms_conditions character varying COLLATE pg_catalog."default",
    source_name character varying COLLATE pg_catalog."default",
    source_url character varying COLLATE pg_catalog."default",
    order_date date,
    geom geometry(MultiPolygon,4326),
    created_user character varying COLLATE pg_catalog."default",
    date_created date,
    last_edited_user character varying COLLATE pg_catalog."default",
    date_edited date,
    announce_date date,
    appl_deadline date,
    regions character varying COLLATE pg_catalog."default",
    rn_guid character varying COLLATE pg_catalog."default",
    resources_parsed json,
    rfgf_gos_reg_num character varying COLLATE pg_catalog."default"
);


CREATE OR REPLACE VIEW torgi_gov_ru.lotcards_spatial_all
 AS
 SELECT row_number() OVER (ORDER BY lc.gid) AS gid,
    lc."noticeNumber" AS "Номер уведомления",
    lc."lotName" AS "Название лота",
    lc."lotStatus" AS "Статус",
    lc."biddType_name" AS "Тип",
    lc."biddForm_name" AS "Форма",
    lc."lotDescription" AS "Описание",
    lc."priceMin" AS "Начальная цена",
    lc."priceFin" AS "Итоговая цена",
    (((lc."biddEndTime" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS "Срок подачи заявок",
    lc."mineralResource" AS "Полезное ископаемое",
    lc."resourceCategory" AS "Категория участков недр",
    lc."squareMR" AS "Площадь",
    lc."resourcePotential" AS "Сведения о запасах и ресурсах",
    lc."resourceAreaId" AS "Идентификатор участка",
    lc."currencyCode" AS "Код валюты",
    lc."etpCode" AS "Код ЭТП",
    (((lc."auctionStartDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS "Дата проведения аукциона",
    (((lc."createDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS "Дата публикации",
    lc."miningSiteName_EA(N)" AS "Наименование участка недр",
    lc."resourceTypeUse_EA(N)" AS "Вид пользования недрами",
    lc."resourceLocation_EA(N)" AS "Местонахождение участка недр",
    lc."conditionsOfUse_EA(N)" AS "Основные условия пользования",
    lc."licensePeriod_EA(N)" AS "Срок пользования участком недр",
    lc."licenseFeeAmount_EA(N)" AS "Размер государственной пошлины за",
    lc."licenseProcedureMaking_EA(N)" AS "Срок и порядок внесения государст",
    lc."participationFee_EA(N)" AS "Размер сбора за участие",
    lc."feeProcedureMaking_EA(N)" AS "Срок и порядок внесения сбора",
    lc."oneTimePaymentProcedure_EA(N)" AS "Срок и порядок внесения окончател",
    lc."depositTimeAndRules_EA(N)" AS "Срок и порядок внесения задатка",
    lc."depositRefund_EA(N)" AS "Срок и порядок возврата задатка",
    ('https://torgi.gov.ru/new/public/lots/lot/'::text || lc.id::text) || '/(lotInfo:info)?fromRec=false'::text AS url,
    lc.rfgf_gos_reg_num,
    lc.rn_guid,
    lb.date_register AS rfgf_date_register,
    lb.date_license_stop,
    lb.user_info AS lic_user_info,
    lb.rfgf_link,
    oil_a.oil_a[0]::numeric AS oil_a,
    replace(oil_a.oil_a[1]::text, '"'::text, ''::text) AS oil_a_units,
    oil_b1.oil_b1[0]::numeric AS oil_b1,
    replace(oil_b1.oil_b1[1]::text, '"'::text, ''::text) AS oil_b1_units,
    oil_b2.oil_b2[0]::numeric AS oil_b2,
    replace(oil_b2.oil_b2[1]::text, '"'::text, ''::text) AS oil_b2_units,
    oil_c1.oil_c1[0]::numeric AS oil_c1,
    replace(oil_c1.oil_c1[1]::text, '"'::text, ''::text) AS oil_c1_units,
    oil_c2.oil_c2[0]::numeric AS oil_c2,
    replace(oil_c2.oil_c2[1]::text, '"'::text, ''::text) AS oil_c2_units,
    oil_d0.oil_d0[0]::numeric AS oil_d0,
    replace(oil_d0.oil_d0[1]::text, '"'::text, ''::text) AS oil_d0_units,
    oil_dl.oil_dl[0]::numeric AS oil_dl,
    replace(oil_dl.oil_dl[1]::text, '"'::text, ''::text) AS oil_dl_units,
    oil_d1.oil_d1[0]::numeric AS oil_d1,
    replace(oil_d1.oil_d1[1]::text, '"'::text, ''::text) AS oil_d1_units,
    oil_d2.oil_d2[0]::numeric AS oil_d2,

    replace(oil_d2.oil_d2[1]::text, '"'::text, ''::text) AS oil_d2_units,
    gas_a.gas_a[0]::numeric AS gas_a,
    replace(gas_a.gas_a[1]::text, '"'::text, ''::text) AS gas_a_units,
    gas_b1.gas_b1[0]::numeric AS gas_b1,
    replace(gas_b1.gas_b1[1]::text, '"'::text, ''::text) AS gas_b1_units,
    gas_b2.gas_b2[0]::numeric AS gas_b2,
    replace(gas_b2.gas_b2[1]::text, '"'::text, ''::text) AS gas_b2_units,
    gas_c1.gas_c1[0]::numeric AS gas_c1,
    replace(gas_c1.gas_c1[1]::text, '"'::text, ''::text) AS gas_c1_units,
    gas_c2.gas_c2[0]::numeric AS gas_c2,
    replace(gas_c2.gas_c2[1]::text, '"'::text, ''::text) AS gas_c2_units,
    gas_d0.gas_d0[0]::numeric AS gas_d0,
    replace(gas_d0.gas_d0[1]::text, '"'::text, ''::text) AS gas_d0_units,
    gas_dl.gas_dl[0]::numeric AS gas_dl,
    replace(gas_dl.gas_dl[1]::text, '"'::text, ''::text) AS gas_dl_units,
    gas_d1.gas_d1[0]::numeric AS gas_d1,
    replace(gas_d1.gas_d1[1]::text, '"'::text, ''::text) AS gas_d1_units,
    gas_d2.gas_d2[0]::numeric AS gas_d2,
    replace(gas_d2.gas_d2[1]::text, '"'::text, ''::text) AS gas_d2_units,
    cond_a.cond_a[0]::numeric AS cond_a,
    replace(cond_a.cond_a[1]::text, '"'::text, ''::text) AS cond_a_units,
    cond_b1.cond_b1[0]::numeric AS cond_b1,
    replace(cond_b1.cond_b1[1]::text, '"'::text, ''::text) AS cond_b1_units,
    cond_b2.cond_b2[0]::numeric AS cond_b2,
    replace(cond_b2.cond_b2[1]::text, '"'::text, ''::text) AS cond_b2_units,
    cond_c1.cond_c1[0]::numeric AS cond_c1,
    replace(cond_c1.cond_c1[1]::text, '"'::text, ''::text) AS cond_c1_units,
    cond_c2.cond_c2[0]::numeric AS cond_c2,
    replace(cond_c2.cond_c2[1]::text, '"'::text, ''::text) AS cond_c2_units,
    cond_d0.cond_d0[0]::numeric AS cond_d0,
    replace(cond_d0.cond_d0[1]::text, '"'::text, ''::text) AS cond_d0_units,
    cond_dl.cond_dl[0]::numeric AS cond_dl,
    replace(cond_dl.cond_dl[1]::text, '"'::text, ''::text) AS cond_dl_units,
    cond_d1.cond_d1[0]::numeric AS cond_d1,
    replace(cond_d1.cond_d1[1]::text, '"'::text, ''::text) AS cond_d1_units,
    cond_d2.cond_d2[0]::numeric AS cond_d2,
    replace(cond_d2.cond_d2[1]::text, '"'::text, ''::text) AS cond_d2_units,
    rn.geom
   FROM torgi_gov_ru.lotcards lc
     JOIN rosnedra.license_blocks_rosnedra_orders rn ON rn.rn_guid::text = lc.rn_guid::text
     LEFT JOIN rfgf.license_blocks_rfgf lb ON lb.gos_reg_num::text = lc.rfgf_gos_reg_num::text
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."oil"."A"'::jsonpath) oil_a(oil_a) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."oil"."B1"'::jsonpath) oil_b1(oil_b1) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."oil"."B2"'::jsonpath) oil_b2(oil_b2) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."oil"."C1"'::jsonpath) oil_c1(oil_c1) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."oil"."C2"'::jsonpath) oil_c2(oil_c2) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."oil"."D0"'::jsonpath) oil_d0(oil_d0) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."oil"."Dл"'::jsonpath) oil_dl(oil_dl) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."oil"."D1"'::jsonpath) oil_d1(oil_d1) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."oil"."D2"'::jsonpath) oil_d2(oil_d2) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."gas"."A"'::jsonpath) gas_a(gas_a) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."gas"."B1"'::jsonpath) gas_b1(gas_b1) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."gas"."B2"'::jsonpath) gas_b2(gas_b2) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."gas"."C1"'::jsonpath) gas_c1(gas_c1) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."gas"."C2"'::jsonpath) gas_c2(gas_c2) ON true

     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."gas"."D0"'::jsonpath) gas_d0(gas_d0) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."gas"."Dл"'::jsonpath) gas_dl(gas_dl) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."gas"."D1"'::jsonpath) gas_d1(gas_d1) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."gas"."D2"'::jsonpath) gas_d2(gas_d2) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."cond"."A"'::jsonpath) cond_a(cond_a) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."cond"."B1"'::jsonpath) cond_b1(cond_b1) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."cond"."B2"'::jsonpath) cond_b2(cond_b2) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."cond"."C1"'::jsonpath) cond_c1(cond_c1) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."cond"."C2"'::jsonpath) cond_c2(cond_c2) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."cond"."D0"'::jsonpath) cond_d0(cond_d0) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."cond"."Dл"'::jsonpath) cond_dl(cond_dl) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."cond"."D1"'::jsonpath) cond_d1(cond_d1) ON true
     LEFT JOIN LATERAL jsonb_path_query(rn.resources_parsed::jsonb, '$."cond"."D2"'::jsonpath) cond_d2(cond_d2) ON true
  ORDER BY lc."miningSiteName_EA(N)";

CREATE OR REPLACE VIEW torgi_gov_ru.lotcards_spatial_licensed
 AS
 SELECT row_number() OVER (ORDER BY lc.gid) AS gid,
    lc."noticeNumber" AS "Номер уведомления",
    lc."lotName" AS "Название лота",
    lc."lotStatus" AS "Статус",
    lc."biddType_name" AS "Тип",
    lc."biddForm_name" AS "Форма",
    lc."lotDescription" AS "Описание",
    lc."priceMin" AS "Начальная цена",
    lc."priceFin" AS "Итоговая цена",
    (((lc."auctionStartDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS "Дата проведения аукциона",
    (((lc."biddEndTime" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS "Срок подачи заявок",
    lc."mineralResource" AS "Полезное ископаемое",
    lc."resourceCategory" AS "Категория участков недр",
    lc."squareMR" AS "Площадь",
    lc."resourcePotential" AS "Сведения о запасах и ресурсах",
    lc."resourceAreaId" AS "Идентификатор участка",
    lc."currencyCode" AS "Код валюты",
    lc."etpCode" AS "Код ЭТП",
    (((lc."createDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS "Дата публикации",
    lc."miningSiteName_EA(N)" AS "Наименование участка недр",
    lc."resourceTypeUse_EA(N)" AS "Вид пользования недрами",
    lc."resourceLocation_EA(N)" AS "Местонахождение участка недр",
    lc."conditionsOfUse_EA(N)" AS "Основные условия пользования",
    lc."licensePeriod_EA(N)" AS "Срок пользования участком недр",
    lc."licenseFeeAmount_EA(N)" AS "Размер государственной пошлины за",
    lc."licenseProcedureMaking_EA(N)" AS "Срок и порядок внесения государст",
    lc."participationFee_EA(N)" AS "Размер сбора за участие",
    lc."feeProcedureMaking_EA(N)" AS "Срок и порядок внесения сбора",
    lc."oneTimePaymentProcedure_EA(N)" AS "Срок и порядок внесения окончател",
    lc."depositTimeAndRules_EA(N)" AS "Срок и порядок внесения задатка",
    lc."depositRefund_EA(N)" AS "Срок и порядок возврата задатка",
    ('https://torgi.gov.ru/new/public/lots/lot/'::text || lc.id::text) || '/(lotInfo:info)?fromRec=false'::text AS url,
    lc.rfgf_gos_reg_num,
    lc.rn_guid,
    lb.date_register AS rfgf_date_register,
    lb.date_license_stop,
    lb.user_info AS lic_user_info,
    lb.rfgf_link,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric AS oil_a,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[1]'::jsonpath)::text) AS oil_a_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric AS oil_b1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[1]'::jsonpath)::text) AS oil_b1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric AS oil_b2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[1]'::jsonpath)::text) AS oil_b2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric AS oil_c1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[1]'::jsonpath)::text) AS oil_c1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric AS oil_c2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[1]'::jsonpath)::text) AS oil_c2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D0"[0]'::jsonpath)::numeric AS oil_d0,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D0"[1]'::jsonpath)::text) AS oil_d0_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."Dл"[0]'::jsonpath)::numeric AS oil_dl,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."Dл"[1]'::jsonpath)::text) AS oil_dl_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D1"[0]'::jsonpath)::numeric AS oil_d1,

    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D1"[1]'::jsonpath)::text) AS oil_d1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D2"[0]'::jsonpath)::numeric AS oil_d2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D2"[1]'::jsonpath)::text) AS oil_d2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric AS gas_a,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[1]'::jsonpath)::text) AS gas_a_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric AS gas_b1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[1]'::jsonpath)::text) AS gas_b1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric AS gas_b2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[1]'::jsonpath)::text) AS gas_b2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric AS gas_c1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[1]'::jsonpath)::text) AS gas_c1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric AS gas_c2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[1]'::jsonpath)::text) AS gas_c2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D0"[0]'::jsonpath)::numeric AS gas_d0,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D0"[1]'::jsonpath)::text) AS gas_d0_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."Dл"[0]'::jsonpath)::numeric AS gas_dl,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."Dл"[1]'::jsonpath)::text) AS gas_dl_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D1"[0]'::jsonpath)::numeric AS gas_d1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D1"[1]'::jsonpath)::text) AS gas_d1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D2"[0]'::jsonpath)::numeric AS gas_d2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D2"[1]'::jsonpath)::text) AS gas_d2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric AS cond_a,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[1]'::jsonpath)::text) AS cond_a_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric AS cond_b1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[1]'::jsonpath)::text) AS cond_b1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric AS cond_b2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[1]'::jsonpath)::text) AS cond_b2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric AS cond_c1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[1]'::jsonpath)::text) AS cond_c1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric AS cond_c2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[1]'::jsonpath)::text) AS cond_c2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D0"[0]'::jsonpath)::numeric AS cond_d0,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D0"[1]'::jsonpath)::text) AS cond_d0_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."Dл"[0]'::jsonpath)::numeric AS cond_dl,

    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."Dл"[1]'::jsonpath)::text) AS cond_dl_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D1"[0]'::jsonpath)::numeric AS cond_d1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D1"[1]'::jsonpath)::text) AS cond_d1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D2"[0]'::jsonpath)::numeric AS cond_d2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D2"[1]'::jsonpath)::text) AS cond_d2_units,
    rn.geom
   FROM torgi_gov_ru.lotcards lc
     JOIN rosnedra.license_blocks_rosnedra_orders rn ON rn.rn_guid::text = lc.rn_guid::text
     JOIN rfgf.license_blocks_rfgf lb ON lb.gos_reg_num::text = lc.rfgf_gos_reg_num::text
  WHERE lc.rfgf_gos_reg_num IS NOT NULL
  ORDER BY lc."miningSiteName_EA(N)";


CREATE OR REPLACE VIEW torgi_gov_ru.lotcards_spatial_view
 AS
 SELECT row_number() OVER (ORDER BY lc.gid) AS gid,
    lc."noticeNumber" AS "Номер уведомления",
    lc."lotName" AS "Название лота",
    lc."lotStatus" AS "Статус",
    lc."biddType_name" AS "Тип",
    lc."biddForm_name" AS "Форма",
    lc."lotDescription" AS "Описание",
    lc."priceMin" AS "Начальная цена",
    lc."priceFin" AS "Итоговая цена",
    (((lc."auctionStartDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS "Дата проведения аукциона",
    (((lc."biddEndTime" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS "Срок подачи заявок",
    lc."mineralResource" AS "Полезное ископаемое",
    lc."resourceCategory" AS "Категория участков недр",
    lc."squareMR" AS "Площадь",
    lc."resourcePotential" AS "Сведения о запасах и ресурсах",
    lc."resourceAreaId" AS "Идентификатор участка",
    lc."currencyCode" AS "Код валюты",
    lc."etpCode" AS "Код ЭТП",
    (((lc."createDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS "Дата публикации",
    lc."miningSiteName_EA(N)" AS "Наименование участка недр",
    lc."resourceTypeUse_EA(N)" AS "Вид пользования недрами",
    lc."resourceLocation_EA(N)" AS "Местонахождение участка недр",
    lc."conditionsOfUse_EA(N)" AS "Основные условия пользования",
    lc."licensePeriod_EA(N)" AS "Срок пользования участком недр",
    lc."licenseFeeAmount_EA(N)" AS "Размер государственной пошлины за",
    lc."licenseProcedureMaking_EA(N)" AS "Срок и порядок внесения государст",
    lc."participationFee_EA(N)" AS "Размер сбора за участие",
    lc."feeProcedureMaking_EA(N)" AS "Срок и порядок внесения сбора",
    lc."oneTimePaymentProcedure_EA(N)" AS "Срок и порядок внесения окончател",
    lc."depositTimeAndRules_EA(N)" AS "Срок и порядок внесения задатка",
    lc."depositRefund_EA(N)" AS "Срок и порядок возврата задатка",
    ('https://torgi.gov.ru/new/public/lots/lot/'::text || lc.id::text) || '/(lotInfo:info)?fromRec=false'::text AS url,
    lc.rn_guid,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric AS oil_a,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[1]'::jsonpath)::text) AS oil_a_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric AS oil_b1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[1]'::jsonpath)::text) AS oil_b1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric AS oil_b2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[1]'::jsonpath)::text) AS oil_b2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric AS oil_c1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[1]'::jsonpath)::text) AS oil_c1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric AS oil_c2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[1]'::jsonpath)::text) AS oil_c2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D0"[0]'::jsonpath)::numeric AS oil_d0,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D0"[1]'::jsonpath)::text) AS oil_d0_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."Dл"[0]'::jsonpath)::numeric AS oil_dl,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."Dл"[1]'::jsonpath)::text) AS oil_dl_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D1"[0]'::jsonpath)::numeric AS oil_d1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D1"[1]'::jsonpath)::text) AS oil_d1_units,

    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D2"[0]'::jsonpath)::numeric AS oil_d2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D2"[1]'::jsonpath)::text) AS oil_d2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric AS gas_a,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[1]'::jsonpath)::text) AS gas_a_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric AS gas_b1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[1]'::jsonpath)::text) AS gas_b1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric AS gas_b2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[1]'::jsonpath)::text) AS gas_b2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric AS gas_c1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[1]'::jsonpath)::text) AS gas_c1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric AS gas_c2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[1]'::jsonpath)::text) AS gas_c2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D0"[0]'::jsonpath)::numeric AS gas_d0,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D0"[1]'::jsonpath)::text) AS gas_d0_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."Dл"[0]'::jsonpath)::numeric AS gas_dl,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."Dл"[1]'::jsonpath)::text) AS gas_dl_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D1"[0]'::jsonpath)::numeric AS gas_d1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D1"[1]'::jsonpath)::text) AS gas_d1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D2"[0]'::jsonpath)::numeric AS gas_d2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D2"[1]'::jsonpath)::text) AS gas_d2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric AS cond_a,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[1]'::jsonpath)::text) AS cond_a_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric AS cond_b1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[1]'::jsonpath)::text) AS cond_b1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric AS cond_b2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[1]'::jsonpath)::text) AS cond_b2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric AS cond_c1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[1]'::jsonpath)::text) AS cond_c1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric AS cond_c2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[1]'::jsonpath)::text) AS cond_c2_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D0"[0]'::jsonpath)::numeric AS cond_d0,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D0"[1]'::jsonpath)::text) AS cond_d0_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."Dл"[0]'::jsonpath)::numeric AS cond_dl,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."Dл"[1]'::jsonpath)::text) AS cond_dl_units,

    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D1"[0]'::jsonpath)::numeric AS cond_d1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D1"[1]'::jsonpath)::text) AS cond_d1_units,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D2"[0]'::jsonpath)::numeric AS cond_d2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D2"[1]'::jsonpath)::text) AS cond_d2_units,
    rn.geom
   FROM torgi_gov_ru.lotcards lc
     JOIN rosnedra.license_blocks_rosnedra_orders rn ON rn.rn_guid::text = lc.rn_guid::text
  WHERE lc.rfgf_gos_reg_num IS NULL
  ORDER BY lc."miningSiteName_EA(N)";


CREATE OR REPLACE VIEW rosnedra.license_rosnedra_orders_view
 AS
 SELECT DISTINCT ON (o.gid) o.gid,
    o.resource_type,
    split_part(o.name::text, chr(10), 1) AS name,
    o.name AS fullname,
    o.area_km,
    o.reserves_predicted_resources,
    o.exp_protocol,
    o.usage_type,
        CASE
            WHEN o.lend_type::text = ANY (ARRAY['1'::character varying::text, '2'::character varying::text, '3'::character varying::text, '4'::character varying::text, '5'::character varying::text, '6'::character varying::text, 'nan'::character varying::text]) THEN NULL::character varying
            ELSE o.lend_type
        END AS lend_type,
    o.planned_terms_conditions,
    (regexp_split_to_array(o.planned_terms_conditions::text, '[\r\n]|\s{3,}'::text))[1] AS planned_time,
    (regexp_split_to_array(o.planned_terms_conditions::text, '[\r\n]|\s{3,}'::text))[2] AS organizer,
    o.source_name,
    o.source_url,
    o.order_date,
    o.geom,
    o.created_user,
    o.date_created,
    o.last_edited_user,
    o.date_edited,
    o.announce_date,
    o.appl_deadline,
    o.appl_deadline - CURRENT_DATE AS days_to_deadline,
    o.regions,
    regexp_replace(regexp_replace(array_to_string(regexp_match(o.name::text, '.есторождени.:\n([^\n]+)\).?\n|.есторождени.:\n([^\n]+) *.?\n|.есторождени.: *(.+)\)'::text), ''::text), '\s{2,}'::text, ' '::text, 'g'::text), '\n'::text, ''::text, 'g'::text) AS fields,
    array_to_string(regexp_split_to_array(regexp_replace(regexp_replace(array_to_string(regexp_match(o.name::text, '.труктур.:(.+)\).? *\n|.труктур.: *\n([^\n]+)\n|.труктур.: *\n([^\n]+)\)|\((.+) структура\)|.труктур.: *([^\n]+)\n'::text), ''::text), '\n'::text, ''::text, 'g'::text), ' {2,}'::text, ' '::text, 'g'::text), ', *'::text), ', '::text) AS structures,
    o.rn_guid,
    o.rfgf_gos_reg_num,
        CASE
            WHEN bool(lc.gid) THEN true
            ELSE false
        END AS has_lot,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric AS oil_a,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."A"[1]'::jsonpath)::text) AS oil_a_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric AS oil_b1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."B1"[1]'::jsonpath)::text) AS oil_b1_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric AS oil_b2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."B2"[1]'::jsonpath)::text) AS oil_b2_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric AS oil_c1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."C1"[1]'::jsonpath)::text) AS oil_c1_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric AS oil_c2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."C2"[1]'::jsonpath)::text) AS oil_c2_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."D0"[0]'::jsonpath)::numeric AS oil_d0,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."D0"[1]'::jsonpath)::text) AS oil_d0_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."Dл"[0]'::jsonpath)::numeric AS oil_dl,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."Dл"[1]'::jsonpath)::text) AS oil_dl_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."D1"[0]'::jsonpath)::numeric AS oil_d1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."D1"[1]'::jsonpath)::text) AS oil_d1_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."D2"[0]'::jsonpath)::numeric AS oil_d2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."oil"."D2"[1]'::jsonpath)::text) AS oil_d2_units,

    jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric AS gas_a,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."A"[1]'::jsonpath)::text) AS gas_a_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric AS gas_b1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."B1"[1]'::jsonpath)::text) AS gas_b1_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric AS gas_b2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."B2"[1]'::jsonpath)::text) AS gas_b2_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric AS gas_c1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."C1"[1]'::jsonpath)::text) AS gas_c1_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric AS gas_c2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."C2"[1]'::jsonpath)::text) AS gas_c2_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."D0"[0]'::jsonpath)::numeric AS gas_d0,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."D0"[1]'::jsonpath)::text) AS gas_d0_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."Dл"[0]'::jsonpath)::numeric AS gas_dl,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."Dл"[1]'::jsonpath)::text) AS gas_dl_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."D1"[0]'::jsonpath)::numeric AS gas_d1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."D1"[1]'::jsonpath)::text) AS gas_d1_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."D2"[0]'::jsonpath)::numeric AS gas_d2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."gas"."D2"[1]'::jsonpath)::text) AS gas_d2_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric AS cond_a,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."A"[1]'::jsonpath)::text) AS cond_a_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric AS cond_b1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."B1"[1]'::jsonpath)::text) AS cond_b1_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric AS cond_b2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."B2"[1]'::jsonpath)::text) AS cond_b2_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric AS cond_c1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."C1"[1]'::jsonpath)::text) AS cond_c1_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric AS cond_c2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."C2"[1]'::jsonpath)::text) AS cond_c2_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."D0"[0]'::jsonpath)::numeric AS cond_d0,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."D0"[1]'::jsonpath)::text) AS cond_d0_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."Dл"[0]'::jsonpath)::numeric AS cond_dl,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."Dл"[1]'::jsonpath)::text) AS cond_dl_units,
    jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."D1"[0]'::jsonpath)::numeric AS cond_d1,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."D1"[1]'::jsonpath)::text) AS cond_d1_units,

    jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."D2"[0]'::jsonpath)::numeric AS cond_d2,
    TRIM(BOTH '"'::text FROM jsonb_path_query_first(o.resources_parsed::jsonb, '$."cond"."D2"[1]'::jsonpath)::text) AS cond_d2_units
   FROM rosnedra.license_blocks_rosnedra_orders o
     LEFT JOIN torgi_gov_ru.lotcards lc ON lc.rn_guid::text = o.rn_guid::text
  WHERE date_part('year'::text, o.order_date) >= (date_part('year'::text, CURRENT_DATE) - 1::double precision) AND (lower(o.resource_type::text) ~~ '%нефть%'::text OR lower(o.resource_type::text) ~~ '%газ%'::text OR lower(o.resource_type::text) ~~ '%конденсат%'::text OR o.resource_type::text = '1'::text);



CREATE OR REPLACE VIEW rosnedra.rn_orders_hcs_active_v
 AS
 WITH rn_active AS (
         SELECT row_number() OVER () AS gid,
            regexp_replace(split_part(rn.name::text, chr(10), 1), '[\r\n]'::text, ' '::text, 'g'::text) AS name,
            regexp_replace(rn.regions::text, '[\r\n]'::text, ' '::text, 'g'::text) AS regions,
            rn.area_km,
            regexp_replace(regexp_replace(regexp_replace(array_to_string(regexp_match(rn.name::text, '.есторождени.:\n([^\n]+)\).?\n|.есторождени.:\n([^\n]+) *.?\n|.есторождени.: *(.+)\)'::text), ''::text), '\s{2,}'::text, ' '::text, 'g'::text), '\n'::text, ''::text, 'g'::text), '[\r\n]'::text, ' '::text, 'g'::text) AS fields,
            regexp_replace(array_to_string(regexp_split_to_array(regexp_replace(regexp_replace(array_to_string(regexp_match(rn.name::text, '.труктур.:(.+)\).? *\n|.труктур.: *\n([^\n]+)\n|.труктур.: *\n([^\n]+)\)|\((.+) структура\)|.труктур.: *([^\n]+)\n'::text), ''::text), '\n'::text, ''::text, 'g'::text), ' {2,}'::text, ' '::text, 'g'::text), ', *'::text), ', '::text), '[\r\n]'::text, ' '::text, 'g'::text) AS structures,
            regexp_replace(regexp_replace(regexp_replace(rn.resource_type::text, '[\r\n]'::text, ' '::text, 'g'::text), ' {2,}'::text, ' '::text, 'g'::text), ' (?=,)'::text, ''::text, 'g'::text) AS resource_type,
                CASE
                    WHEN (regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}|(?<=\d{4}) '::text))[1] = ANY (ARRAY['1'::text, '2'::text, '3'::text, '4'::text]) THEN NULL::text
                    ELSE (regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}|(?<=\d{4}) '::text))[1]
                END AS planned_time,
            regexp_replace((regexp_split_to_array(regexp_replace(rn.planned_terms_conditions::text, '(?<=\d{4})\s?г\.?|(?<=\d{4})\s?года'::text, ''::text, 'g'::text), '[\r\n]|\s{3,}|(?<=\d{4})\W*'::text))[2], '[\r\n]'::text, ' '::text, 'g'::text) AS organizer,
                CASE
                    WHEN rn.appl_deadline IS NOT NULL THEN rn.appl_deadline
                    WHEN lc."biddEndTime"::date IS NOT NULL THEN lc."biddEndTime"::date
                    ELSE NULL::date
                END AS appl_deadline,
                CASE
                    WHEN regexp_like(lower(regexp_replace(rn.usage_type::text, '[\r\n]'::text, ' '::text, 'g'::text)), '.*разведк.*добыч.*'::text, 'i'::text) THEN 'геологическое изучение недр, разведка и добыча полезных ископаемых'::text
                    ELSE 'геологическое изучение недр'::text
                END AS usage_type,
                CASE
                    WHEN rn.lend_type::text = ANY (ARRAY['1'::character varying::text, '2'::character varying::text, '3'::character varying::text, '4'::character varying::text, '5'::character varying::text, '6'::character varying::text, 'nan'::character varying::text]) THEN NULL::character varying
                    ELSE rn.lend_type
                END AS lend_type,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS oil_ab1c1,
                CASE

                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS oil_b2c2,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D0"[0]'::jsonpath)::numeric AS oil_d0,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."Dл"[0]'::jsonpath)::numeric AS oil_dl,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D1"[0]'::jsonpath)::numeric AS oil_d1,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D2"[0]'::jsonpath)::numeric AS oil_d2,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS gas_ab1c1,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS gas_b2c2,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D0"[0]'::jsonpath)::numeric AS gas_d0,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."Dл"[0]'::jsonpath)::numeric AS gas_dl,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D1"[0]'::jsonpath)::numeric AS gas_d1,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D2"[0]'::jsonpath)::numeric AS gas_d2,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS cond_ab1c1,
                CASE

                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS cond_b2c2,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D0"[0]'::jsonpath)::numeric AS cond_d0,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."Dл"[0]'::jsonpath)::numeric AS cond_dl,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D1"[0]'::jsonpath)::numeric AS cond_d1,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D2"[0]'::jsonpath)::numeric AS cond_d2,
            rn.source_url AS order_url,
            rn.source_name AS order_name,
            rn.order_date,
            rn.appl_deadline - CURRENT_DATE AS days_to_deadline,
                CASE
                    WHEN lc.gid IS NOT NULL THEN true
                    ELSE false
                END AS has_lot,
            lc."lotStatus" AS auction_lot_status,
                CASE
                    WHEN lc."priceMin" IS NULL THEN ''::text
                    ELSE concat(TRIM(BOTH FROM to_char(lc."priceMin", '999 999 999 999D99'::text)), ' ', chr(8381))
                END AS auction_price_min,
                CASE
                    WHEN lc."priceFin" IS NULL THEN ''::text
                    ELSE concat(TRIM(BOTH FROM to_char(lc."priceFin", '999 999 999 999D99'::text)), ' ', chr(8381))
                END AS auction_price_fin,
            ('https://torgi.gov.ru/new/public/lots/lot/'::text || lc.id::text) || '/(lotInfo:info)?fromRec=false'::text AS lot_url,
            (((lc."createDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_publish_datetime,
            (((lc."biddEndTime" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_appl_end_datetime,
            (((lc."auctionStartDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_start_datetime,
            lc."createDate"::date AS auct_create_date,
            lc."biddEndTime"::date AS auct_appl_end_date,
            lc."auctionStartDate"::date AS auct_start_date,
            CURRENT_DATE - lc."auctionStartDate"::date AS days_from_auct,
            regexp_replace(rn.rfgf_gos_reg_num::text, '[\r\n]'::text, ' '::text, 'g'::text) AS rfgf_gos_reg_num,
            rfgf.user_info AS rfgf_license_user,
            rfgf.date_register AS rfgf_date_register,
            rfgf.date_license_stop AS rfgf_date_stop,
            rfgf.rfgf_link AS rfgf_url,
            rn.geom
           FROM rosnedra.license_blocks_rosnedra_orders rn
             LEFT JOIN torgi_gov_ru.lotcards lc ON lc.rn_guid::text = rn.rn_guid::text
             LEFT JOIN rfgf.license_blocks_rfgf rfgf ON rfgf.gos_reg_num::text = rn.rfgf_gos_reg_num::text
          WHERE date_part('year'::text, rn.order_date) >= (date_part('year'::text, CURRENT_DATE) - 1::double precision) AND (lower(rn.resource_type::text) ~~ '%нефть%'::text OR lower(rn.resource_type::text) ~~ '%газ%'::text OR lower(rn.resource_type::text) ~~ '%конденсат%'::text OR rn.resource_type::text = '1'::text) AND (lc."createDate" IS NULL OR date_part('year'::text, lc."createDate") >= (date_part('year'::text, CURRENT_DATE) - 1::double precision) AND lc."createDate" >= rn.order_date)
        )
 SELECT rn_active.gid,
    rn_active.name,
    rn_active.regions,
    rn_active.area_km,
    rn_active.fields,
    rn_active.structures,
    rn_active.resource_type,
    rn_active.planned_time,
    rn_active.organizer,
    rn_active.appl_deadline,

    rn_active.usage_type,
    rn_active.lend_type,
    rn_active.oil_ab1c1,
    rn_active.oil_b2c2,
    rn_active.oil_d0,
    rn_active.oil_dl,
    rn_active.oil_d1,
    rn_active.oil_d2,
    rn_active.gas_ab1c1,
    rn_active.gas_b2c2,
    rn_active.gas_d0,
    rn_active.gas_dl,
    rn_active.gas_d1,
    rn_active.gas_d2,
    rn_active.cond_ab1c1,
    rn_active.cond_b2c2,
    rn_active.cond_d0,
    rn_active.cond_dl,
    rn_active.cond_d1,
    rn_active.cond_d2,
    rn_active.order_url,
    rn_active.order_name,
    rn_active.order_date,
    rn_active.days_to_deadline,
    rn_active.has_lot,
    rn_active.auction_lot_status,
    rn_active.auction_price_min,
    rn_active.auction_price_fin,
    rn_active.lot_url,
    rn_active.auct_publish_datetime,
    rn_active.auct_appl_end_datetime,
    rn_active.auct_start_datetime,
    rn_active.auct_create_date,
    rn_active.auct_appl_end_date,
    rn_active.auct_start_date,
    rn_active.days_from_auct,
    rn_active.rfgf_gos_reg_num,
    rn_active.rfgf_license_user,
    rn_active.rfgf_date_register,
    rn_active.rfgf_date_stop,
    rn_active.rfgf_url,
    rn_active.geom
   FROM rn_active
  WHERE rn_active.usage_type ~* '.*разведк.*добыч.*'::text AND rn_active.rfgf_gos_reg_num IS NULL AND rn_active.has_lot AND (rn_active.auction_lot_status::text = ANY (ARRAY['APPLICATIONS_SUBMISSION'::character varying::text, 'PUBLISHED'::character varying::text])) AND date_part('year'::text, rn_active.order_date) = date_part('year'::text, now()) OR rn_active.usage_type !~* '.*разведк.*добыч.*'::text AND rn_active.rfgf_gos_reg_num IS NULL AND date_part('year'::text, rn_active.order_date) = date_part('year'::text, now()) AND rn_active.appl_deadline IS NOT NULL AND rn_active.days_to_deadline > 0;


CREATE OR REPLACE VIEW rosnedra.rn_orders_hcs_all_v
 AS
 SELECT row_number() OVER () AS gid,
    regexp_replace(split_part(rn.name::text, chr(10), 1), '[\r\n]'::text, ' '::text, 'g'::text) AS name,
    regexp_replace(rn.regions::text, '[\r\n]'::text, ' '::text, 'g'::text) AS regions,
    rn.area_km,
    regexp_replace(regexp_replace(regexp_replace(array_to_string(regexp_match(rn.name::text, '.есторождени.:\n([^\n]+)\).?\n|.есторождени.:\n([^\n]+) *.?\n|.есторождени.: *(.+)\)'::text), ''::text), '\s{2,}'::text, ' '::text, 'g'::text), '\n'::text, ''::text, 'g'::text), '[\r\n]'::text, ' '::text, 'g'::text) AS fields,
    regexp_replace(array_to_string(regexp_split_to_array(regexp_replace(regexp_replace(array_to_string(regexp_match(rn.name::text, '.труктур.:(.+)\).? *\n|.труктур.: *\n([^\n]+)\n|.труктур.: *\n([^\n]+)\)|\((.+) структура\)|.труктур.: *([^\n]+)\n'::text), ''::text), '\n'::text, ''::text, 'g'::text), ' {2,}'::text, ' '::text, 'g'::text), ', *'::text), ', '::text), '[\r\n]'::text, ' '::text, 'g'::text) AS structures,
    regexp_replace(regexp_replace(regexp_replace(rn.resource_type::text, '[\r\n]'::text, ' '::text, 'g'::text), ' {2,}'::text, ' '::text, 'g'::text), ' (?=,)'::text, ''::text, 'g'::text) AS resource_type,
        CASE
            WHEN (regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}'::text))[1] = ANY (ARRAY['1'::text, '2'::text, '3'::text, '4'::text]) THEN NULL::text
            ELSE (regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}'::text))[1]
        END AS planned_time,
    regexp_replace((regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}'::text))[2], '[\r\n]'::text, ' '::text, 'g'::text) AS organizer,
    rn.appl_deadline,
    regexp_replace(rn.usage_type::text, '[\r\n]'::text, ' '::text, 'g'::text) AS usage_type,
        CASE
            WHEN rn.lend_type::text = ANY (ARRAY['1'::character varying::text, '2'::character varying::text, '3'::character varying::text, '4'::character varying::text, '5'::character varying::text, '6'::character varying::text, 'nan'::character varying::text]) THEN NULL::character varying
            ELSE rn.lend_type
        END AS lend_type,
        CASE
            WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric, 0::numeric)
            ELSE NULL::numeric
        END AS oil_ab1c1,
        CASE
            WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric, 0::numeric)
            ELSE NULL::numeric
        END AS oil_b2c2,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D0"[0]'::jsonpath)::numeric AS oil_d0,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."Dл"[0]'::jsonpath)::numeric AS oil_dl,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D1"[0]'::jsonpath)::numeric AS oil_d1,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D2"[0]'::jsonpath)::numeric AS oil_d2,
        CASE

            WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric, 0::numeric)
            ELSE NULL::numeric
        END AS gas_ab1c1,
        CASE
            WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric, 0::numeric)
            ELSE NULL::numeric
        END AS gas_b2c2,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D0"[0]'::jsonpath)::numeric AS gas_d0,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."Dл"[0]'::jsonpath)::numeric AS gas_dl,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D1"[0]'::jsonpath)::numeric AS gas_d1,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D2"[0]'::jsonpath)::numeric AS gas_d2,
        CASE
            WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric, 0::numeric)
            ELSE NULL::numeric
        END AS cond_ab1c1,
        CASE
            WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric, 0::numeric)
            ELSE NULL::numeric
        END AS cond_b2c2,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D0"[0]'::jsonpath)::numeric AS cond_d0,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."Dл"[0]'::jsonpath)::numeric AS cond_dl,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D1"[0]'::jsonpath)::numeric AS cond_d1,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D2"[0]'::jsonpath)::numeric AS cond_d2,
    rn.source_url AS order_url,
    rn.source_name AS order_name,
    rn.order_date,
    rn.appl_deadline - CURRENT_DATE AS days_to_deadline,
        CASE
            WHEN lc.gid IS NOT NULL THEN true
            ELSE false
        END AS has_lot,
    lc."lotStatus" AS auction_lot_status,
        CASE
            WHEN lc."priceMin" IS NULL THEN ''::text
            ELSE concat(TRIM(BOTH FROM to_char(lc."priceMin", '999 999 999 999D99'::text)), ' ', chr(8381))
        END AS auction_price_min,
        CASE
            WHEN lc."priceFin" IS NULL THEN ''::text

            ELSE concat(TRIM(BOTH FROM to_char(lc."priceFin", '999 999 999 999D99'::text)), ' ', chr(8381))
        END AS auction_price_fin,
    ('https://torgi.gov.ru/new/public/lots/lot/'::text || lc.id::text) || '/(lotInfo:info)?fromRec=false'::text AS lot_url,
    (((lc."createDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_publish_datetime,
    (((lc."biddEndTime" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_appl_end_datetime,
    (((lc."auctionStartDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_start_datetime,
    lc."createDate"::date AS auct_create_date,
    lc."biddEndTime"::date AS auct_appl_end_date,
    lc."auctionStartDate"::date AS auct_start_date,
    CURRENT_DATE - lc."auctionStartDate"::date AS days_from_auct,
    regexp_replace(rn.rfgf_gos_reg_num::text, '[\r\n]'::text, ' '::text, 'g'::text) AS rfgf_gos_reg_num,
    rfgf.user_info AS rfgf_license_user,
    rfgf.date_register AS rfgf_date_register,
    rfgf.date_license_stop AS rfgf_date_stop,
    rfgf.rfgf_link AS rfgf_url,
    rn.geom
   FROM rosnedra.license_blocks_rosnedra_orders rn
     LEFT JOIN torgi_gov_ru.lotcards lc ON lc.rn_guid::text = rn.rn_guid::text
     LEFT JOIN rfgf.license_blocks_rfgf rfgf ON rfgf.gos_reg_num::text = rn.rfgf_gos_reg_num::text
  WHERE date_part('year'::text, rn.order_date) >= (date_part('year'::text, CURRENT_DATE) - 1::double precision) AND (lower(rn.resource_type::text) ~~ '%нефть%'::text OR lower(rn.resource_type::text) ~~ '%газ%'::text OR lower(rn.resource_type::text) ~~ '%конденсат%'::text OR rn.resource_type::text = '1'::text) AND (lc."createDate" IS NULL OR date_part('year'::text, lc."createDate") >= (date_part('year'::text, CURRENT_DATE) - 1::double precision) AND lc."createDate" >= rn.order_date);


CREATE OR REPLACE VIEW rosnedra.rn_orders_hcs_compl_v
 AS
 WITH rn_orders AS (
         SELECT row_number() OVER () AS gid,
            regexp_replace(split_part(rn.name::text, chr(10), 1), '[\r\n]'::text, ' '::text, 'g'::text) AS name,
            regexp_replace(rn.regions::text, '[\r\n]'::text, ' '::text, 'g'::text) AS regions,
            rn.area_km,
            regexp_replace(regexp_replace(regexp_replace(array_to_string(regexp_match(rn.name::text, '.есторождени.:\n([^\n]+)\).?\n|.есторождени.:\n([^\n]+) *.?\n|.есторождени.: *(.+)\)'::text), ''::text), '\s{2,}'::text, ' '::text, 'g'::text), '\n'::text, ''::text, 'g'::text), '[\r\n]'::text, ' '::text, 'g'::text) AS fields,
            regexp_replace(array_to_string(regexp_split_to_array(regexp_replace(regexp_replace(array_to_string(regexp_match(rn.name::text, '.труктур.:(.+)\).? *\n|.труктур.: *\n([^\n]+)\n|.труктур.: *\n([^\n]+)\)|\((.+) структура\)|.труктур.: *([^\n]+)\n'::text), ''::text), '\n'::text, ''::text, 'g'::text), ' {2,}'::text, ' '::text, 'g'::text), ', *'::text), ', '::text), '[\r\n]'::text, ' '::text, 'g'::text) AS structures,
            regexp_replace(regexp_replace(regexp_replace(rn.resource_type::text, '[\r\n]'::text, ' '::text, 'g'::text), ' {2,}'::text, ' '::text, 'g'::text), ' (?=,)'::text, ''::text, 'g'::text) AS resource_type,
                CASE
                    WHEN (regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}|(?<=\d{4}) '::text))[1] = ANY (ARRAY['1'::text, '2'::text, '3'::text, '4'::text]) THEN NULL::text
                    ELSE (regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}|(?<=\d{4}) '::text))[1]
                END AS planned_time,
            regexp_replace((regexp_split_to_array(regexp_replace(rn.planned_terms_conditions::text, '(?<=\d{4})\s?г\.?|(?<=\d{4})\s?года'::text, ''::text, 'g'::text), '[\r\n]|\s{3,}|(?<=\d{4})\W*'::text))[2], '[\r\n]'::text, ' '::text, 'g'::text) AS organizer,
                CASE
                    WHEN rn.appl_deadline IS NOT NULL THEN rn.appl_deadline
                    WHEN lc."biddEndTime"::date IS NOT NULL THEN lc."biddEndTime"::date
                    ELSE NULL::date
                END AS appl_deadline,
            regexp_replace(rn.usage_type::text, '[\r\n]'::text, ' '::text, 'g'::text) AS usage_type,
                CASE
                    WHEN rn.lend_type::text = ANY (ARRAY['1'::character varying::text, '2'::character varying::text, '3'::character varying::text, '4'::character varying::text, '5'::character varying::text, '6'::character varying::text, 'nan'::character varying::text]) THEN NULL::character varying
                    ELSE rn.lend_type
                END AS lend_type,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS oil_ab1c1,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric

                END AS oil_b2c2,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D0"[0]'::jsonpath)::numeric AS oil_d0,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."Dл"[0]'::jsonpath)::numeric AS oil_dl,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D1"[0]'::jsonpath)::numeric AS oil_d1,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D2"[0]'::jsonpath)::numeric AS oil_d2,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS gas_ab1c1,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS gas_b2c2,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D0"[0]'::jsonpath)::numeric AS gas_d0,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."Dл"[0]'::jsonpath)::numeric AS gas_dl,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D1"[0]'::jsonpath)::numeric AS gas_d1,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D2"[0]'::jsonpath)::numeric AS gas_d2,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS cond_ab1c1,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS cond_b2c2,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D0"[0]'::jsonpath)::numeric AS cond_d0,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."Dл"[0]'::jsonpath)::numeric AS cond_dl,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D1"[0]'::jsonpath)::numeric AS cond_d1,

            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D2"[0]'::jsonpath)::numeric AS cond_d2,
            rn.source_url AS order_url,
            rn.source_name AS order_name,
            rn.order_date,
            rn.appl_deadline - CURRENT_DATE AS days_to_deadline,
                CASE
                    WHEN lc.gid IS NOT NULL THEN true
                    ELSE false
                END AS has_lot,
            lc."lotStatus" AS auction_lot_status,
                CASE
                    WHEN lc."priceMin" IS NULL THEN ''::text
                    ELSE concat(TRIM(BOTH FROM to_char(lc."priceMin", '999 999 999 999D99'::text)), ' ', chr(8381))
                END AS auction_price_min,
                CASE
                    WHEN lc."priceFin" IS NULL THEN ''::text
                    ELSE concat(TRIM(BOTH FROM to_char(lc."priceFin", '999 999 999 999D99'::text)), ' ', chr(8381))
                END AS auction_price_fin,
            ('https://torgi.gov.ru/new/public/lots/lot/'::text || lc.id::text) || '/(lotInfo:info)?fromRec=false'::text AS lot_url,
            (((lc."createDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_publish_datetime,
            (((lc."biddEndTime" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_appl_end_datetime,
            (((lc."auctionStartDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_start_datetime,
            lc."createDate"::date AS auct_create_date,
            lc."biddEndTime"::date AS auct_appl_end_date,
            lc."auctionStartDate"::date AS auct_start_date,
            CURRENT_DATE - lc."auctionStartDate"::date AS days_from_auct,
            regexp_replace(rn.rfgf_gos_reg_num::text, '[\r\n]'::text, ' '::text, 'g'::text) AS rfgf_gos_reg_num,
            rfgf.user_info AS rfgf_license_user,
            rfgf.date_register AS rfgf_date_register,
            rfgf.date_license_stop AS rfgf_date_stop,
            rfgf.rfgf_link AS rfgf_url,
            rn.geom
           FROM rosnedra.license_blocks_rosnedra_orders rn
             LEFT JOIN torgi_gov_ru.lotcards lc ON lc.rn_guid::text = rn.rn_guid::text
             LEFT JOIN rfgf.license_blocks_rfgf rfgf ON rfgf.gos_reg_num::text = rn.rfgf_gos_reg_num::text
          WHERE date_part('year'::text, rn.order_date) >= (date_part('year'::text, CURRENT_DATE) - 1::double precision) AND (lower(rn.resource_type::text) ~~ '%нефть%'::text OR lower(rn.resource_type::text) ~~ '%газ%'::text OR lower(rn.resource_type::text) ~~ '%конденсат%'::text OR rn.resource_type::text = '1'::text) AND (lc."createDate" IS NULL OR date_part('year'::text, lc."createDate") >= (date_part('year'::text, CURRENT_DATE) - 1::double precision) AND lc."createDate" >= rn.order_date)
        )
 SELECT rn_orders.gid,
    rn_orders.name,
    rn_orders.regions,
    rn_orders.area_km,
    rn_orders.fields,
    rn_orders.structures,
    rn_orders.resource_type,
    rn_orders.planned_time,
    rn_orders.organizer,
    rn_orders.appl_deadline,
    rn_orders.usage_type,
    rn_orders.lend_type,
    rn_orders.oil_ab1c1,
    rn_orders.oil_b2c2,
    rn_orders.oil_d0,
    rn_orders.oil_dl,
    rn_orders.oil_d1,
    rn_orders.oil_d2,
    rn_orders.gas_ab1c1,
    rn_orders.gas_b2c2,
    rn_orders.gas_d0,
    rn_orders.gas_dl,
    rn_orders.gas_d1,
    rn_orders.gas_d2,
    rn_orders.cond_ab1c1,
    rn_orders.cond_b2c2,
    rn_orders.cond_d0,
    rn_orders.cond_dl,
    rn_orders.cond_d1,
    rn_orders.cond_d2,
    rn_orders.order_url,
    rn_orders.order_name,
    rn_orders.order_date,
    rn_orders.days_to_deadline,
    rn_orders.has_lot,
    rn_orders.auction_lot_status,
    rn_orders.auction_price_min,
    rn_orders.auction_price_fin,
    rn_orders.lot_url,
    rn_orders.auct_publish_datetime,
    rn_orders.auct_appl_end_datetime,
    rn_orders.auct_start_datetime,
    rn_orders.auct_create_date,
    rn_orders.auct_appl_end_date,

    rn_orders.auct_start_date,
    rn_orders.days_from_auct,
    rn_orders.rfgf_gos_reg_num,
    rn_orders.rfgf_license_user,
    rn_orders.rfgf_date_register,
    rn_orders.rfgf_date_stop,
    rn_orders.rfgf_url,
    rn_orders.geom
   FROM rn_orders
  WHERE rn_orders.usage_type ~* '.*разведк.*добыч.*'::text AND date_part('year'::text, rn_orders.order_date) >= (date_part('year'::text, now()) - 1::double precision) AND rn_orders.has_lot AND ((rn_orders.auction_lot_status::text = ANY (ARRAY['SUCCEED'::character varying::text])) AND rn_orders.days_from_auct < 60 OR rn_orders.rfgf_gos_reg_num IS NOT NULL) OR rn_orders.usage_type !~* '.*разведк.*добыч.*'::text AND date_part('year'::text, rn_orders.order_date) >= (date_part('year'::text, now()) - 1::double precision) AND rn_orders.rfgf_gos_reg_num IS NOT NULL;



CREATE OR REPLACE VIEW rosnedra.rn_orders_hcs_determ_v
 AS
 WITH rn_orders AS (
         SELECT row_number() OVER () AS gid,
            regexp_replace(split_part(rn.name::text, chr(10), 1), '[\r\n]'::text, ' '::text, 'g'::text) AS name,
            regexp_replace(rn.regions::text, '[\r\n]'::text, ' '::text, 'g'::text) AS regions,
            rn.area_km,
            regexp_replace(regexp_replace(regexp_replace(array_to_string(regexp_match(rn.name::text, '.есторождени.:\n([^\n]+)\).?\n|.есторождени.:\n([^\n]+) *.?\n|.есторождени.: *(.+)\)'::text), ''::text), '\s{2,}'::text, ' '::text, 'g'::text), '\n'::text, ''::text, 'g'::text), '[\r\n]'::text, ' '::text, 'g'::text) AS fields,
            regexp_replace(array_to_string(regexp_split_to_array(regexp_replace(regexp_replace(array_to_string(regexp_match(rn.name::text, '.труктур.:(.+)\).? *\n|.труктур.: *\n([^\n]+)\n|.труктур.: *\n([^\n]+)\)|\((.+) структура\)|.труктур.: *([^\n]+)\n'::text), ''::text), '\n'::text, ''::text, 'g'::text), ' {2,}'::text, ' '::text, 'g'::text), ', *'::text), ', '::text), '[\r\n]'::text, ' '::text, 'g'::text) AS structures,
            regexp_replace(regexp_replace(regexp_replace(rn.resource_type::text, '[\r\n]'::text, ' '::text, 'g'::text), ' {2,}'::text, ' '::text, 'g'::text), ' (?=,)'::text, ''::text, 'g'::text) AS resource_type,
                CASE
                    WHEN (regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}|(?<=\d{4}) '::text))[1] = ANY (ARRAY['1'::text, '2'::text, '3'::text, '4'::text]) THEN NULL::text
                    ELSE (regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}|(?<=\d{4}) '::text))[1]
                END AS planned_time,
            regexp_replace((regexp_split_to_array(regexp_replace(rn.planned_terms_conditions::text, '(?<=\d{4})\s?г\.?|(?<=\d{4})\s?года'::text, ''::text, 'g'::text), '[\r\n]|\s{3,}|(?<=\d{4})\W*'::text))[2], '[\r\n]'::text, ' '::text, 'g'::text) AS organizer,
                CASE
                    WHEN rn.appl_deadline IS NOT NULL THEN rn.appl_deadline
                    WHEN lc."biddEndTime"::date IS NOT NULL THEN lc."biddEndTime"::date
                    ELSE NULL::date
                END AS appl_deadline,
            regexp_replace(rn.usage_type::text, '[\r\n]'::text, ' '::text, 'g'::text) AS usage_type,
                CASE
                    WHEN rn.lend_type::text = ANY (ARRAY['1'::character varying::text, '2'::character varying::text, '3'::character varying::text, '4'::character varying::text, '5'::character varying::text, '6'::character varying::text, 'nan'::character varying::text]) THEN NULL::character varying
                    ELSE rn.lend_type
                END AS lend_type,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS oil_ab1c1,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric

                END AS oil_b2c2,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D0"[0]'::jsonpath)::numeric AS oil_d0,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."Dл"[0]'::jsonpath)::numeric AS oil_dl,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D1"[0]'::jsonpath)::numeric AS oil_d1,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D2"[0]'::jsonpath)::numeric AS oil_d2,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS gas_ab1c1,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS gas_b2c2,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D0"[0]'::jsonpath)::numeric AS gas_d0,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."Dл"[0]'::jsonpath)::numeric AS gas_dl,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D1"[0]'::jsonpath)::numeric AS gas_d1,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D2"[0]'::jsonpath)::numeric AS gas_d2,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS cond_ab1c1,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS cond_b2c2,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D0"[0]'::jsonpath)::numeric AS cond_d0,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."Dл"[0]'::jsonpath)::numeric AS cond_dl,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D1"[0]'::jsonpath)::numeric AS cond_d1,

            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D2"[0]'::jsonpath)::numeric AS cond_d2,
            rn.source_url AS order_url,
            rn.source_name AS order_name,
            rn.order_date,
            rn.appl_deadline - CURRENT_DATE AS days_to_deadline,
                CASE
                    WHEN lc.gid IS NOT NULL THEN true
                    ELSE false
                END AS has_lot,
            lc."lotStatus" AS auction_lot_status,
                CASE
                    WHEN lc."priceMin" IS NULL THEN ''::text
                    ELSE concat(TRIM(BOTH FROM to_char(lc."priceMin", '999 999 999 999D99'::text)), ' ', chr(8381))
                END AS auction_price_min,
                CASE
                    WHEN lc."priceFin" IS NULL THEN ''::text
                    ELSE concat(TRIM(BOTH FROM to_char(lc."priceFin", '999 999 999 999D99'::text)), ' ', chr(8381))
                END AS auction_price_fin,
            ('https://torgi.gov.ru/new/public/lots/lot/'::text || lc.id::text) || '/(lotInfo:info)?fromRec=false'::text AS lot_url,
            (((lc."createDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_publish_datetime,
            (((lc."biddEndTime" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_appl_end_datetime,
            (((lc."auctionStartDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_start_datetime,
            lc."createDate"::date AS auct_create_date,
            lc."biddEndTime"::date AS auct_appl_end_date,
            lc."auctionStartDate"::date AS auct_start_date,
            CURRENT_DATE - lc."auctionStartDate"::date AS days_from_auct,
            regexp_replace(rn.rfgf_gos_reg_num::text, '[\r\n]'::text, ' '::text, 'g'::text) AS rfgf_gos_reg_num,
            rfgf.user_info AS rfgf_license_user,
            rfgf.date_register AS rfgf_date_register,
            rfgf.date_license_stop AS rfgf_date_stop,
            rfgf.rfgf_link AS rfgf_url,
            rn.geom
           FROM rosnedra.license_blocks_rosnedra_orders rn
             LEFT JOIN torgi_gov_ru.lotcards lc ON lc.rn_guid::text = rn.rn_guid::text
             LEFT JOIN rfgf.license_blocks_rfgf rfgf ON rfgf.gos_reg_num::text = rn.rfgf_gos_reg_num::text
          WHERE date_part('year'::text, rn.order_date) >= (date_part('year'::text, CURRENT_DATE) - 1::double precision) AND (lower(rn.resource_type::text) ~~ '%нефть%'::text OR lower(rn.resource_type::text) ~~ '%газ%'::text OR lower(rn.resource_type::text) ~~ '%конденсат%'::text OR rn.resource_type::text = '1'::text) AND (lc."createDate" IS NULL OR date_part('year'::text, lc."createDate") >= (date_part('year'::text, CURRENT_DATE) - 1::double precision) AND lc."createDate" >= rn.order_date)
        )
 SELECT rn_orders.gid,
    rn_orders.name,
    rn_orders.regions,
    rn_orders.area_km,
    rn_orders.fields,
    rn_orders.structures,
    rn_orders.resource_type,
    rn_orders.planned_time,
    rn_orders.organizer,
    rn_orders.appl_deadline,
    rn_orders.usage_type,
    rn_orders.lend_type,
    rn_orders.oil_ab1c1,
    rn_orders.oil_b2c2,
    rn_orders.oil_d0,
    rn_orders.oil_dl,
    rn_orders.oil_d1,
    rn_orders.oil_d2,
    rn_orders.gas_ab1c1,
    rn_orders.gas_b2c2,
    rn_orders.gas_d0,
    rn_orders.gas_dl,
    rn_orders.gas_d1,
    rn_orders.gas_d2,
    rn_orders.cond_ab1c1,
    rn_orders.cond_b2c2,
    rn_orders.cond_d0,
    rn_orders.cond_dl,
    rn_orders.cond_d1,
    rn_orders.cond_d2,
    rn_orders.order_url,
    rn_orders.order_name,
    rn_orders.order_date,
    rn_orders.days_to_deadline,
    rn_orders.has_lot,
    rn_orders.auction_lot_status,
    rn_orders.auction_price_min,
    rn_orders.auction_price_fin,
    rn_orders.lot_url,
    rn_orders.auct_publish_datetime,
    rn_orders.auct_appl_end_datetime,
    rn_orders.auct_start_datetime,
    rn_orders.auct_create_date,
    rn_orders.auct_appl_end_date,

    rn_orders.auct_start_date,
    rn_orders.days_from_auct,
    rn_orders.rfgf_gos_reg_num,
    rn_orders.rfgf_license_user,
    rn_orders.rfgf_date_register,
    rn_orders.rfgf_date_stop,
    rn_orders.rfgf_url,
    rn_orders.geom
   FROM rn_orders
  WHERE rn_orders.usage_type ~* '.*разведк.*добыч.*'::text AND date_part('year'::text, rn_orders.order_date) >= date_part('year'::text, now()) AND rn_orders.has_lot AND ((rn_orders.auction_lot_status::text = ANY (ARRAY['DETERMINING_WINNER'::character varying::text])) OR (rn_orders.auction_lot_status::text = ANY (ARRAY['FAILED'::character varying::text])) AND rn_orders.days_from_auct < 60) OR rn_orders.usage_type !~* '.*разведк.*добыч.*'::text AND date_part('year'::text, rn_orders.order_date) >= (date_part('year'::text, now()) - 1::double precision) AND rn_orders.rfgf_gos_reg_num IS NULL AND rn_orders.days_to_deadline > '-90'::integer AND rn_orders.days_to_deadline <= 0;




CREATE OR REPLACE VIEW rosnedra.rn_orders_hcs_potent_v
 AS
 WITH rn_orders AS (
         SELECT row_number() OVER () AS gid,
            regexp_replace(split_part(rn.name::text, chr(10), 1), '[\r\n]'::text, ' '::text, 'g'::text) AS name,
            regexp_replace(rn.regions::text, '[\r\n]'::text, ' '::text, 'g'::text) AS regions,
            rn.area_km,
            regexp_replace(regexp_replace(regexp_replace(array_to_string(regexp_match(rn.name::text, '.есторождени.:\n([^\n]+)\).?\n|.есторождени.:\n([^\n]+) *.?\n|.есторождени.: *(.+)\)'::text), ''::text), '\s{2,}'::text, ' '::text, 'g'::text), '\n'::text, ''::text, 'g'::text), '[\r\n]'::text, ' '::text, 'g'::text) AS fields,
            regexp_replace(array_to_string(regexp_split_to_array(regexp_replace(regexp_replace(array_to_string(regexp_match(rn.name::text, '.труктур.:(.+)\).? *\n|.труктур.: *\n([^\n]+)\n|.труктур.: *\n([^\n]+)\)|\((.+) структура\)|.труктур.: *([^\n]+)\n'::text), ''::text), '\n'::text, ''::text, 'g'::text), ' {2,}'::text, ' '::text, 'g'::text), ', *'::text), ', '::text), '[\r\n]'::text, ' '::text, 'g'::text) AS structures,
            regexp_replace(regexp_replace(regexp_replace(rn.resource_type::text, '[\r\n]'::text, ' '::text, 'g'::text), ' {2,}'::text, ' '::text, 'g'::text), ' (?=,)'::text, ''::text, 'g'::text) AS resource_type,
                CASE
                    WHEN (regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}|(?<=\d{4}) '::text))[1] = ANY (ARRAY['1'::text, '2'::text, '3'::text, '4'::text]) THEN NULL::text
                    ELSE (regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}|(?<=\d{4}) '::text))[1]
                END AS planned_time,
            regexp_replace((regexp_split_to_array(regexp_replace(rn.planned_terms_conditions::text, '(?<=\d{4})\s?г\.?|(?<=\d{4})\s?года'::text, ''::text, 'g'::text), '[\r\n]|\s{3,}|(?<=\d{4})\W*'::text))[2], '[\r\n]'::text, ' '::text, 'g'::text) AS organizer,
                CASE
                    WHEN rn.appl_deadline IS NOT NULL THEN rn.appl_deadline
                    WHEN lc."biddEndTime"::date IS NOT NULL THEN lc."biddEndTime"::date
                    ELSE NULL::date
                END AS appl_deadline,
            regexp_replace(rn.usage_type::text, '[\r\n]'::text, ' '::text, 'g'::text) AS usage_type,
                CASE
                    WHEN rn.lend_type::text = ANY (ARRAY['1'::character varying::text, '2'::character varying::text, '3'::character varying::text, '4'::character varying::text, '5'::character varying::text, '6'::character varying::text, 'nan'::character varying::text]) THEN NULL::character varying
                    ELSE rn.lend_type
                END AS lend_type,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS oil_ab1c1,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric

                END AS oil_b2c2,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D0"[0]'::jsonpath)::numeric AS oil_d0,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."Dл"[0]'::jsonpath)::numeric AS oil_dl,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D1"[0]'::jsonpath)::numeric AS oil_d1,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D2"[0]'::jsonpath)::numeric AS oil_d2,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS gas_ab1c1,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS gas_b2c2,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D0"[0]'::jsonpath)::numeric AS gas_d0,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."Dл"[0]'::jsonpath)::numeric AS gas_dl,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D1"[0]'::jsonpath)::numeric AS gas_d1,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D2"[0]'::jsonpath)::numeric AS gas_d2,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS cond_ab1c1,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS cond_b2c2,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D0"[0]'::jsonpath)::numeric AS cond_d0,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."Dл"[0]'::jsonpath)::numeric AS cond_dl,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D1"[0]'::jsonpath)::numeric AS cond_d1,

            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D2"[0]'::jsonpath)::numeric AS cond_d2,
            rn.source_url AS order_url,
            rn.source_name AS order_name,
            rn.order_date,
            rn.appl_deadline - CURRENT_DATE AS days_to_deadline,
                CASE
                    WHEN lc.gid IS NOT NULL THEN true
                    ELSE false
                END AS has_lot,
            lc."lotStatus" AS auction_lot_status,
                CASE
                    WHEN lc."priceMin" IS NULL THEN ''::text
                    ELSE concat(TRIM(BOTH FROM to_char(lc."priceMin", '999 999 999 999D99'::text)), ' ', chr(8381))
                END AS auction_price_min,
                CASE
                    WHEN lc."priceFin" IS NULL THEN ''::text
                    ELSE concat(TRIM(BOTH FROM to_char(lc."priceFin", '999 999 999 999D99'::text)), ' ', chr(8381))
                END AS auction_price_fin,
            ('https://torgi.gov.ru/new/public/lots/lot/'::text || lc.id::text) || '/(lotInfo:info)?fromRec=false'::text AS lot_url,
            (((lc."createDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_publish_datetime,
            (((lc."biddEndTime" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_appl_end_datetime,
            (((lc."auctionStartDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_start_datetime,
            lc."createDate"::date AS auct_create_date,
            lc."biddEndTime"::date AS auct_appl_end_date,
            lc."auctionStartDate"::date AS auct_start_date,
            CURRENT_DATE - lc."auctionStartDate"::date AS days_from_auct,
            regexp_replace(rn.rfgf_gos_reg_num::text, '[\r\n]'::text, ' '::text, 'g'::text) AS rfgf_gos_reg_num,
            rfgf.user_info AS rfgf_license_user,
            rfgf.date_register AS rfgf_date_register,
            rfgf.date_license_stop AS rfgf_date_stop,
            rfgf.rfgf_link AS rfgf_url,
            rn.geom
           FROM rosnedra.license_blocks_rosnedra_orders rn
             LEFT JOIN torgi_gov_ru.lotcards lc ON lc.rn_guid::text = rn.rn_guid::text
             LEFT JOIN rfgf.license_blocks_rfgf rfgf ON rfgf.gos_reg_num::text = rn.rfgf_gos_reg_num::text
          WHERE date_part('year'::text, rn.order_date) >= (date_part('year'::text, CURRENT_DATE) - 1::double precision) AND (lower(rn.resource_type::text) ~~ '%нефть%'::text OR lower(rn.resource_type::text) ~~ '%газ%'::text OR lower(rn.resource_type::text) ~~ '%конденсат%'::text OR rn.resource_type::text = '1'::text) AND (lc."createDate" IS NULL OR date_part('year'::text, lc."createDate") >= (date_part('year'::text, CURRENT_DATE) - 1::double precision) AND lc."createDate" >= rn.order_date)
        )
 SELECT rn_orders.gid,
    rn_orders.name,
    rn_orders.regions,
    rn_orders.area_km,
    rn_orders.fields,
    rn_orders.structures,
    rn_orders.resource_type,
    rn_orders.planned_time,
    rn_orders.organizer,
    rn_orders.appl_deadline,
    rn_orders.usage_type,
    rn_orders.lend_type,
    rn_orders.oil_ab1c1,
    rn_orders.oil_b2c2,
    rn_orders.oil_d0,
    rn_orders.oil_dl,
    rn_orders.oil_d1,
    rn_orders.oil_d2,
    rn_orders.gas_ab1c1,
    rn_orders.gas_b2c2,
    rn_orders.gas_d0,
    rn_orders.gas_dl,
    rn_orders.gas_d1,
    rn_orders.gas_d2,
    rn_orders.cond_ab1c1,
    rn_orders.cond_b2c2,
    rn_orders.cond_d0,
    rn_orders.cond_dl,
    rn_orders.cond_d1,
    rn_orders.cond_d2,
    rn_orders.order_url,
    rn_orders.order_name,
    rn_orders.order_date,
    rn_orders.days_to_deadline,
    rn_orders.has_lot,
    rn_orders.auction_lot_status,
    rn_orders.auction_price_min,
    rn_orders.auction_price_fin,
    rn_orders.lot_url,
    rn_orders.auct_publish_datetime,
    rn_orders.auct_appl_end_datetime,
    rn_orders.auct_start_datetime,
    rn_orders.auct_create_date,
    rn_orders.auct_appl_end_date,

    rn_orders.auct_start_date,
    rn_orders.days_from_auct,
    rn_orders.rfgf_gos_reg_num,
    rn_orders.rfgf_license_user,
    rn_orders.rfgf_date_register,
    rn_orders.rfgf_date_stop,
    rn_orders.rfgf_url,
    rn_orders.geom
   FROM rn_orders
  WHERE rn_orders.usage_type ~* '.*разведк.*добыч.*'::text AND rn_orders.rfgf_gos_reg_num IS NULL AND NOT rn_orders.has_lot AND date_part('year'::text, rn_orders.order_date) = date_part('year'::text, now());



CREATE OR REPLACE VIEW rosnedra.rn_orders_hsc_fail_v
 AS
 WITH rn_orders AS (
         SELECT row_number() OVER () AS gid,
            regexp_replace(split_part(rn.name::text, chr(10), 1), '[\r\n]'::text, ' '::text, 'g'::text) AS name,
            regexp_replace(rn.regions::text, '[\r\n]'::text, ' '::text, 'g'::text) AS regions,
            rn.area_km,
            regexp_replace(regexp_replace(regexp_replace(array_to_string(regexp_match(rn.name::text, '.есторождени.:\n([^\n]+)\).?\n|.есторождени.:\n([^\n]+) *.?\n|.есторождени.: *(.+)\)'::text), ''::text), '\s{2,}'::text, ' '::text, 'g'::text), '\n'::text, ''::text, 'g'::text), '[\r\n]'::text, ' '::text, 'g'::text) AS fields,
            regexp_replace(array_to_string(regexp_split_to_array(regexp_replace(regexp_replace(array_to_string(regexp_match(rn.name::text, '.труктур.:(.+)\).? *\n|.труктур.: *\n([^\n]+)\n|.труктур.: *\n([^\n]+)\)|\((.+) структура\)|.труктур.: *([^\n]+)\n'::text), ''::text), '\n'::text, ''::text, 'g'::text), ' {2,}'::text, ' '::text, 'g'::text), ', *'::text), ', '::text), '[\r\n]'::text, ' '::text, 'g'::text) AS structures,
            regexp_replace(regexp_replace(regexp_replace(rn.resource_type::text, '[\r\n]'::text, ' '::text, 'g'::text), ' {2,}'::text, ' '::text, 'g'::text), ' (?=,)'::text, ''::text, 'g'::text) AS resource_type,
                CASE
                    WHEN (regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}|(?<=\d{4}) '::text))[1] = ANY (ARRAY['1'::text, '2'::text, '3'::text, '4'::text]) THEN NULL::text
                    ELSE (regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}|(?<=\d{4}) '::text))[1]
                END AS planned_time,
            regexp_replace((regexp_split_to_array(regexp_replace(rn.planned_terms_conditions::text, '(?<=\d{4})\s?г\.?|(?<=\d{4})\s?года'::text, ''::text, 'g'::text), '[\r\n]|\s{3,}|(?<=\d{4})\W*'::text))[2], '[\r\n]'::text, ' '::text, 'g'::text) AS organizer,
                CASE
                    WHEN rn.appl_deadline IS NOT NULL THEN rn.appl_deadline
                    WHEN lc."biddEndTime"::date IS NOT NULL THEN lc."biddEndTime"::date
                    ELSE NULL::date
                END AS appl_deadline,
            regexp_replace(rn.usage_type::text, '[\r\n]'::text, ' '::text, 'g'::text) AS usage_type,
                CASE
                    WHEN rn.lend_type::text = ANY (ARRAY['1'::character varying::text, '2'::character varying::text, '3'::character varying::text, '4'::character varying::text, '5'::character varying::text, '6'::character varying::text, 'nan'::character varying::text]) THEN NULL::character varying
                    ELSE rn.lend_type
                END AS lend_type,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS oil_ab1c1,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric

                END AS oil_b2c2,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D0"[0]'::jsonpath)::numeric AS oil_d0,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."Dл"[0]'::jsonpath)::numeric AS oil_dl,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D1"[0]'::jsonpath)::numeric AS oil_d1,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D2"[0]'::jsonpath)::numeric AS oil_d2,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS gas_ab1c1,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS gas_b2c2,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D0"[0]'::jsonpath)::numeric AS gas_d0,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."Dл"[0]'::jsonpath)::numeric AS gas_dl,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D1"[0]'::jsonpath)::numeric AS gas_d1,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D2"[0]'::jsonpath)::numeric AS gas_d2,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS cond_ab1c1,
                CASE
                    WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric, 0::numeric)
                    ELSE NULL::numeric
                END AS cond_b2c2,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D0"[0]'::jsonpath)::numeric AS cond_d0,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."Dл"[0]'::jsonpath)::numeric AS cond_dl,
            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D1"[0]'::jsonpath)::numeric AS cond_d1,

            jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D2"[0]'::jsonpath)::numeric AS cond_d2,
            rn.source_url AS order_url,
            rn.source_name AS order_name,
            rn.order_date,
            rn.appl_deadline - CURRENT_DATE AS days_to_deadline,
                CASE
                    WHEN lc.gid IS NOT NULL THEN true
                    ELSE false
                END AS has_lot,
            lc."lotStatus" AS auction_lot_status,
                CASE
                    WHEN lc."priceMin" IS NULL THEN ''::text
                    ELSE concat(TRIM(BOTH FROM to_char(lc."priceMin", '999 999 999 999D99'::text)), ' ', chr(8381))
                END AS auction_price_min,
                CASE
                    WHEN lc."priceFin" IS NULL THEN ''::text
                    ELSE concat(TRIM(BOTH FROM to_char(lc."priceFin", '999 999 999 999D99'::text)), ' ', chr(8381))
                END AS auction_price_fin,
            ('https://torgi.gov.ru/new/public/lots/lot/'::text || lc.id::text) || '/(lotInfo:info)?fromRec=false'::text AS lot_url,
            (((lc."createDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_publish_datetime,
            (((lc."biddEndTime" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_appl_end_datetime,
            (((lc."auctionStartDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_start_datetime,
            lc."createDate"::date AS auct_create_date,
            lc."biddEndTime"::date AS auct_appl_end_date,
            lc."auctionStartDate"::date AS auct_start_date,
            CURRENT_DATE - lc."auctionStartDate"::date AS days_from_auct,
            regexp_replace(rn.rfgf_gos_reg_num::text, '[\r\n]'::text, ' '::text, 'g'::text) AS rfgf_gos_reg_num,
            rfgf.user_info AS rfgf_license_user,
            rfgf.date_register AS rfgf_date_register,
            rfgf.date_license_stop AS rfgf_date_stop,
            rfgf.rfgf_link AS rfgf_url,
            rn.geom
           FROM rosnedra.license_blocks_rosnedra_orders rn
             LEFT JOIN torgi_gov_ru.lotcards lc ON lc.rn_guid::text = rn.rn_guid::text
             LEFT JOIN rfgf.license_blocks_rfgf rfgf ON rfgf.gos_reg_num::text = rn.rfgf_gos_reg_num::text
          WHERE date_part('year'::text, rn.order_date) >= (date_part('year'::text, CURRENT_DATE) - 1::double precision) AND (lower(rn.resource_type::text) ~~ '%нефть%'::text OR lower(rn.resource_type::text) ~~ '%газ%'::text OR lower(rn.resource_type::text) ~~ '%конденсат%'::text OR rn.resource_type::text = '1'::text) AND (lc."createDate" IS NULL OR date_part('year'::text, lc."createDate") >= (date_part('year'::text, CURRENT_DATE) - 1::double precision) AND lc."createDate" >= rn.order_date)
        )
 SELECT rn_orders.gid,
    rn_orders.name,
    rn_orders.regions,
    rn_orders.area_km,
    rn_orders.fields,
    rn_orders.structures,
    rn_orders.resource_type,
    rn_orders.planned_time,
    rn_orders.organizer,
    rn_orders.appl_deadline,
    rn_orders.usage_type,
    rn_orders.lend_type,
    rn_orders.oil_ab1c1,
    rn_orders.oil_b2c2,
    rn_orders.oil_d0,
    rn_orders.oil_dl,
    rn_orders.oil_d1,
    rn_orders.oil_d2,
    rn_orders.gas_ab1c1,
    rn_orders.gas_b2c2,
    rn_orders.gas_d0,
    rn_orders.gas_dl,
    rn_orders.gas_d1,
    rn_orders.gas_d2,
    rn_orders.cond_ab1c1,
    rn_orders.cond_b2c2,
    rn_orders.cond_d0,
    rn_orders.cond_dl,
    rn_orders.cond_d1,
    rn_orders.cond_d2,
    rn_orders.order_url,
    rn_orders.order_name,
    rn_orders.order_date,
    rn_orders.days_to_deadline,
    rn_orders.has_lot,
    rn_orders.auction_lot_status,
    rn_orders.auction_price_min,
    rn_orders.auction_price_fin,
    rn_orders.lot_url,
    rn_orders.auct_publish_datetime,
    rn_orders.auct_appl_end_datetime,
    rn_orders.auct_start_datetime,
    rn_orders.auct_create_date,
    rn_orders.auct_appl_end_date,

    rn_orders.auct_start_date,
    rn_orders.days_from_auct,
    rn_orders.rfgf_gos_reg_num,
    rn_orders.rfgf_license_user,
    rn_orders.rfgf_date_register,
    rn_orders.rfgf_date_stop,
    rn_orders.rfgf_url,
    rn_orders.geom
   FROM rn_orders
  WHERE rn_orders.usage_type ~* '.*разведк.*добыч.*'::text AND date_part('year'::text, rn_orders.order_date) >= (date_part('year'::text, now()) - 1::double precision) AND rn_orders.has_lot AND (rn_orders.auction_lot_status::text = 'CANCELLED'::text OR rn_orders.auction_lot_status::text = 'FAILED'::text AND rn_orders.rfgf_gos_reg_num IS NULL AND rn_orders.days_from_auct >= 60 OR rn_orders.auction_lot_status::text = 'SUCCEED'::text AND rn_orders.days_from_auct >= 60 AND rn_orders.rfgf_gos_reg_num IS NULL) OR rn_orders.usage_type !~* '.*разведк.*добыч.*'::text AND date_part('year'::text, rn_orders.order_date) >= (date_part('year'::text, now()) - 1::double precision) AND rn_orders.rfgf_gos_reg_num IS NULL AND (rn_orders.days_to_deadline <= '-90'::integer OR rn_orders.appl_deadline IS NULL);




CREATE OR REPLACE VIEW rosnedra.rn_orders_np_report_v
 AS
 WITH rn_orders_np_report AS (
         SELECT ((regexp_match(rn_orders_hcs_active_v.order_name::text, '№ (\d{1,3})'::text))[1] || ' от '::text) || to_char(rn_orders_hcs_active_v.order_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS order_n_date,
            rn_orders_hcs_active_v.name,
            rn_orders_hcs_active_v.regions AS subject_rf,
            rn_orders_hcs_active_v.organizer,
            'НП'::text AS type_of_usage,
            rn_orders_hcs_active_v.lend_type,
            ''::text AS appl_start_date,
            to_char(rn_orders_hcs_active_v.appl_deadline::timestamp with time zone, 'DD.MM.YYYY'::text) AS appl_end_date,
            rn_orders_hcs_active_v.area_km,
            rn_orders_hcs_active_v.fields,
            rn_orders_hcs_active_v.structures,
            rn_orders_hcs_active_v.resource_type,
            rn_orders_hcs_active_v.oil_ab1c1,
            rn_orders_hcs_active_v.oil_b2c2,
            rn_orders_hcs_active_v.oil_d0,
            rn_orders_hcs_active_v.oil_dl,
            rn_orders_hcs_active_v.oil_d1,
            rn_orders_hcs_active_v.oil_d2,
            rn_orders_hcs_active_v.gas_ab1c1,
            rn_orders_hcs_active_v.gas_b2c2,
            rn_orders_hcs_active_v.gas_d0,
            rn_orders_hcs_active_v.gas_dl,
            rn_orders_hcs_active_v.gas_d1,
            rn_orders_hcs_active_v.gas_d2,
            rn_orders_hcs_active_v.cond_ab1c1 + rn_orders_hcs_active_v.cond_b2c2 + rn_orders_hcs_active_v.cond_d0 + rn_orders_hcs_active_v.cond_dl + rn_orders_hcs_active_v.cond_d1 + rn_orders_hcs_active_v.cond_d2 AS cond,
            ''::text AS result,
            ''::text AS comments,
            'Актуальный'::text AS status
           FROM rosnedra.rn_orders_hcs_active_v
          WHERE rn_orders_hcs_active_v.usage_type !~* 'разведка.*добыча'::text
        UNION
         SELECT ((regexp_match(rn_orders_hcs_potent_v.order_name::text, '№ (\d{1,3})'::text))[1] || ' от '::text) || to_char(rn_orders_hcs_potent_v.order_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS order_n_date,
            rn_orders_hcs_potent_v.name,
            rn_orders_hcs_potent_v.regions AS subject_rf,
            rn_orders_hcs_potent_v.organizer,
            'НП'::text AS type_of_usage,
            rn_orders_hcs_potent_v.lend_type,
            ''::text AS appl_start_date,
            to_char(rn_orders_hcs_potent_v.appl_deadline::timestamp with time zone, 'DD.MM.YYYY'::text) AS appl_end_date,
            rn_orders_hcs_potent_v.area_km,
            rn_orders_hcs_potent_v.fields,
            rn_orders_hcs_potent_v.structures,
            rn_orders_hcs_potent_v.resource_type,
            rn_orders_hcs_potent_v.oil_ab1c1,
            rn_orders_hcs_potent_v.oil_b2c2,
            rn_orders_hcs_potent_v.oil_d0,
            rn_orders_hcs_potent_v.oil_dl,
            rn_orders_hcs_potent_v.oil_d1,
            rn_orders_hcs_potent_v.oil_d2,
            rn_orders_hcs_potent_v.gas_ab1c1,
            rn_orders_hcs_potent_v.gas_b2c2,
            rn_orders_hcs_potent_v.gas_d0,
            rn_orders_hcs_potent_v.gas_dl,
            rn_orders_hcs_potent_v.gas_d1,
            rn_orders_hcs_potent_v.gas_d2,
            rn_orders_hcs_potent_v.cond_ab1c1 + rn_orders_hcs_potent_v.cond_b2c2 + rn_orders_hcs_potent_v.cond_d0 + rn_orders_hcs_potent_v.cond_dl + rn_orders_hcs_potent_v.cond_d1 + rn_orders_hcs_potent_v.cond_d2 AS cond,
            ''::text AS result,
            ''::text AS comments,
            'Потенциальный'::text AS status
           FROM rosnedra.rn_orders_hcs_potent_v
          WHERE rn_orders_hcs_potent_v.usage_type !~* 'разведка.*добыча'::text
        UNION
         SELECT ((regexp_match(rn_orders_hcs_compl_v.order_name::text, '№ (\d{1,3})'::text))[1] || ' от '::text) || to_char(rn_orders_hcs_compl_v.order_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS order_n_date,
            rn_orders_hcs_compl_v.name,
            rn_orders_hcs_compl_v.regions AS subject_rf,
            rn_orders_hcs_compl_v.organizer,
            'НП'::text AS type_of_usage,

            rn_orders_hcs_compl_v.lend_type,
            ''::text AS appl_start_date,
            to_char(rn_orders_hcs_compl_v.appl_deadline::timestamp with time zone, 'DD.MM.YYYY'::text) AS appl_end_date,
            rn_orders_hcs_compl_v.area_km,
            rn_orders_hcs_compl_v.fields,
            rn_orders_hcs_compl_v.structures,
            rn_orders_hcs_compl_v.resource_type,
            rn_orders_hcs_compl_v.oil_ab1c1,
            rn_orders_hcs_compl_v.oil_b2c2,
            rn_orders_hcs_compl_v.oil_d0,
            rn_orders_hcs_compl_v.oil_dl,
            rn_orders_hcs_compl_v.oil_d1,
            rn_orders_hcs_compl_v.oil_d2,
            rn_orders_hcs_compl_v.gas_ab1c1,
            rn_orders_hcs_compl_v.gas_b2c2,
            rn_orders_hcs_compl_v.gas_d0,
            rn_orders_hcs_compl_v.gas_dl,
            rn_orders_hcs_compl_v.gas_d1,
            rn_orders_hcs_compl_v.gas_d2,
            rn_orders_hcs_compl_v.cond_ab1c1 + rn_orders_hcs_compl_v.cond_b2c2 + rn_orders_hcs_compl_v.cond_d0 + rn_orders_hcs_compl_v.cond_dl + rn_orders_hcs_compl_v.cond_d1 + rn_orders_hcs_compl_v.cond_d2 AS cond,
            ''::text AS result,
            ''::text AS comments,
            'Завершенный'::text AS status
           FROM rosnedra.rn_orders_hcs_compl_v
          WHERE rn_orders_hcs_compl_v.usage_type !~* 'разведка.*добыча'::text AND date_part('year'::text, rn_orders_hcs_compl_v.order_date) = date_part('year'::text, now())
        UNION
         SELECT ((regexp_match(rn_orders_hcs_determ_v.order_name::text, '№ (\d{1,3})'::text))[1] || ' от '::text) || to_char(rn_orders_hcs_determ_v.order_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS order_n_date,
            rn_orders_hcs_determ_v.name,
            rn_orders_hcs_determ_v.regions AS subject_rf,
            rn_orders_hcs_determ_v.organizer,
            'НП'::text AS type_of_usage,
            rn_orders_hcs_determ_v.lend_type,
            ''::text AS appl_start_date,
            to_char(rn_orders_hcs_determ_v.appl_deadline::timestamp with time zone, 'DD.MM.YYYY'::text) AS appl_end_date,
            rn_orders_hcs_determ_v.area_km,
            rn_orders_hcs_determ_v.fields,
            rn_orders_hcs_determ_v.structures,
            rn_orders_hcs_determ_v.resource_type,
            rn_orders_hcs_determ_v.oil_ab1c1,
            rn_orders_hcs_determ_v.oil_b2c2,
            rn_orders_hcs_determ_v.oil_d0,
            rn_orders_hcs_determ_v.oil_dl,
            rn_orders_hcs_determ_v.oil_d1,
            rn_orders_hcs_determ_v.oil_d2,
            rn_orders_hcs_determ_v.gas_ab1c1,
            rn_orders_hcs_determ_v.gas_b2c2,
            rn_orders_hcs_determ_v.gas_d0,
            rn_orders_hcs_determ_v.gas_dl,
            rn_orders_hcs_determ_v.gas_d1,
            rn_orders_hcs_determ_v.gas_d2,
            rn_orders_hcs_determ_v.cond_ab1c1 + rn_orders_hcs_determ_v.cond_b2c2 + rn_orders_hcs_determ_v.cond_d0 + rn_orders_hcs_determ_v.cond_dl + rn_orders_hcs_determ_v.cond_d1 + rn_orders_hcs_determ_v.cond_d2 AS cond,
            ''::text AS result,
            ''::text AS comments,
            'Определение победителя'::text AS status
           FROM rosnedra.rn_orders_hcs_determ_v
          WHERE rn_orders_hcs_determ_v.usage_type !~* 'разведка.*добыча'::text
        )
 SELECT rn_orders_np_report.order_n_date AS "Приказ №, дата",
    rn_orders_np_report.name AS "Наименование участка недр",
    rn_orders_np_report.subject_rf AS "Субъект РФ",
    rn_orders_np_report.organizer AS "Организатор",
    rn_orders_np_report.type_of_usage AS "Вид использования",
    rn_orders_np_report.lend_type AS "Способ распределения",
    rn_orders_np_report.appl_start_date AS "Дата начала подачи заявки",
    rn_orders_np_report.appl_end_date AS "Дата окончания подачи заявки",
    rn_orders_np_report.area_km AS "S, кв.км",
    rn_orders_np_report.fields AS "Месторождение",
    rn_orders_np_report.structures AS "Подготовленные структуры",
    rn_orders_np_report.resource_type AS "тип УВ",
    rn_orders_np_report.oil_ab1c1 AS "нефть ab1c1, млн.т",
    rn_orders_np_report.oil_b2c2 AS "нефть b2c2, млн.т",
    rn_orders_np_report.oil_d0 AS "нефть d0, млн.т",
    rn_orders_np_report.oil_dl AS "нефть dl, млн.т",
    rn_orders_np_report.oil_d1 AS "нефть d1, млн.т",
    rn_orders_np_report.oil_d2 AS "нефть d2, млн.т",
    rn_orders_np_report.gas_ab1c1 AS "газ ab1c1, млрд.м3",
    rn_orders_np_report.gas_b2c2 AS "газ b2c2, млрд.м3",
    rn_orders_np_report.gas_d0 AS "газ d0, млрд.м3",
    rn_orders_np_report.gas_dl AS "газ dl, млрд.м3",
    rn_orders_np_report.gas_d1 AS "газ d1, млрд.м3",
    rn_orders_np_report.gas_d2 AS "газ d2, млрд.м3",
    rn_orders_np_report.cond AS "конденсат",
    rn_orders_np_report.result AS "Результат",
    rn_orders_np_report.comments AS "Примечания",
    rn_orders_np_report.status AS "Статус"
   FROM rn_orders_np_report
  ORDER BY ((regexp_match(rn_orders_np_report.order_n_date, '(\d{1,3}) от'::text))[1]::numeric), rn_orders_np_report.name;




CREATE OR REPLACE VIEW rosnedra.rn_orders_nr_ne_report_v
 AS
 WITH rn_orders_nr_ne_report AS (
         SELECT ((regexp_match(rn_orders_hcs_active_v.order_name::text, '№ (\d{1,3})'::text))[1] || ' от '::text) || to_char(rn_orders_hcs_active_v.order_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS order_n_date,
            rn_orders_hcs_active_v.name,
            rn_orders_hcs_active_v.regions AS subject_rf,
            rn_orders_hcs_active_v.planned_time,
            rn_orders_hcs_active_v.organizer,
            'НР/НЭ'::text AS type_of_usage,
            rn_orders_hcs_active_v.lend_type,
            to_char(rn_orders_hcs_active_v.auct_create_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS appl_start_date,
            to_char(rn_orders_hcs_active_v.auct_appl_end_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS appl_end_date,
            to_char(rn_orders_hcs_active_v.auct_start_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS auct_date,
            rn_orders_hcs_active_v.auction_price_min,
            rn_orders_hcs_active_v.auction_price_fin,
            rn_orders_hcs_active_v.area_km,
            rn_orders_hcs_active_v.fields,
            rn_orders_hcs_active_v.structures,
            rn_orders_hcs_active_v.resource_type,
            rn_orders_hcs_active_v.oil_ab1c1,
            rn_orders_hcs_active_v.oil_b2c2,
            rn_orders_hcs_active_v.oil_d0,
            rn_orders_hcs_active_v.oil_dl,
            rn_orders_hcs_active_v.oil_d1,
            rn_orders_hcs_active_v.oil_d2,
            rn_orders_hcs_active_v.gas_ab1c1,
            rn_orders_hcs_active_v.gas_b2c2,
            rn_orders_hcs_active_v.gas_d0,
            rn_orders_hcs_active_v.gas_dl,
            rn_orders_hcs_active_v.gas_d1,
            rn_orders_hcs_active_v.gas_d2,
            'Актуальный'::text AS status,
            rn_orders_hcs_active_v.lot_url
           FROM rosnedra.rn_orders_hcs_active_v
          WHERE rn_orders_hcs_active_v.usage_type ~* 'разведка.*добыча'::text
        UNION
         SELECT ((regexp_match(rn_orders_hcs_potent_v.order_name::text, '№ (\d{1,3})'::text))[1] || ' от '::text) || to_char(rn_orders_hcs_potent_v.order_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS order_n_date,
            rn_orders_hcs_potent_v.name,
            rn_orders_hcs_potent_v.regions AS subject_rf,
            rn_orders_hcs_potent_v.planned_time,
            rn_orders_hcs_potent_v.organizer,
            'НР/НЭ'::text AS type_of_usage,
            rn_orders_hcs_potent_v.lend_type,
            to_char(rn_orders_hcs_potent_v.auct_create_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS appl_start_date,
            to_char(rn_orders_hcs_potent_v.auct_appl_end_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS appl_end_date,
            to_char(rn_orders_hcs_potent_v.auct_start_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS auct_date,
            rn_orders_hcs_potent_v.auction_price_min,
            rn_orders_hcs_potent_v.auction_price_fin,
            rn_orders_hcs_potent_v.area_km,
            rn_orders_hcs_potent_v.fields,
            rn_orders_hcs_potent_v.structures,
            rn_orders_hcs_potent_v.resource_type,
            rn_orders_hcs_potent_v.oil_ab1c1,
            rn_orders_hcs_potent_v.oil_b2c2,
            rn_orders_hcs_potent_v.oil_d0,
            rn_orders_hcs_potent_v.oil_dl,
            rn_orders_hcs_potent_v.oil_d1,
            rn_orders_hcs_potent_v.oil_d2,
            rn_orders_hcs_potent_v.gas_ab1c1,
            rn_orders_hcs_potent_v.gas_b2c2,
            rn_orders_hcs_potent_v.gas_d0,
            rn_orders_hcs_potent_v.gas_dl,
            rn_orders_hcs_potent_v.gas_d1,
            rn_orders_hcs_potent_v.gas_d2,
            'Потенциальный'::text AS status,
            rn_orders_hcs_potent_v.lot_url
           FROM rosnedra.rn_orders_hcs_potent_v
          WHERE rn_orders_hcs_potent_v.usage_type ~* 'разведка.*добыча'::text
        UNION

         SELECT ((regexp_match(rn_orders_hcs_compl_v.order_name::text, '№ (\d{1,3})'::text))[1] || ' от '::text) || to_char(rn_orders_hcs_compl_v.order_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS order_n_date,
            rn_orders_hcs_compl_v.name,
            rn_orders_hcs_compl_v.regions AS subject_rf,
            rn_orders_hcs_compl_v.planned_time,
            rn_orders_hcs_compl_v.organizer,
            'НР/НЭ'::text AS type_of_usage,
            rn_orders_hcs_compl_v.lend_type,
            to_char(rn_orders_hcs_compl_v.auct_create_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS appl_start_date,
            to_char(rn_orders_hcs_compl_v.auct_appl_end_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS appl_end_date,
            to_char(rn_orders_hcs_compl_v.auct_start_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS auct_date,
            rn_orders_hcs_compl_v.auction_price_min,
            rn_orders_hcs_compl_v.auction_price_fin,
            rn_orders_hcs_compl_v.area_km,
            rn_orders_hcs_compl_v.fields,
            rn_orders_hcs_compl_v.structures,
            rn_orders_hcs_compl_v.resource_type,
            rn_orders_hcs_compl_v.oil_ab1c1,
            rn_orders_hcs_compl_v.oil_b2c2,
            rn_orders_hcs_compl_v.oil_d0,
            rn_orders_hcs_compl_v.oil_dl,
            rn_orders_hcs_compl_v.oil_d1,
            rn_orders_hcs_compl_v.oil_d2,
            rn_orders_hcs_compl_v.gas_ab1c1,
            rn_orders_hcs_compl_v.gas_b2c2,
            rn_orders_hcs_compl_v.gas_d0,
            rn_orders_hcs_compl_v.gas_dl,
            rn_orders_hcs_compl_v.gas_d1,
            rn_orders_hcs_compl_v.gas_d2,
            'Завершенный'::text AS status,
            rn_orders_hcs_compl_v.lot_url
           FROM rosnedra.rn_orders_hcs_compl_v
          WHERE rn_orders_hcs_compl_v.usage_type ~* 'разведка.*добыча'::text AND date_part('year'::text, rn_orders_hcs_compl_v.order_date) = date_part('year'::text, now())
        UNION
         SELECT ((regexp_match(rn_orders_hcs_determ_v.order_name::text, '№ (\d{1,3})'::text))[1] || ' от '::text) || to_char(rn_orders_hcs_determ_v.order_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS order_n_date,
            rn_orders_hcs_determ_v.name,
            rn_orders_hcs_determ_v.regions AS subject_rf,
            rn_orders_hcs_determ_v.planned_time,
            rn_orders_hcs_determ_v.organizer,
            'НР/НЭ'::text AS type_of_usage,
            rn_orders_hcs_determ_v.lend_type,
            to_char(rn_orders_hcs_determ_v.auct_create_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS appl_start_date,
            to_char(rn_orders_hcs_determ_v.auct_appl_end_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS appl_end_date,
            to_char(rn_orders_hcs_determ_v.auct_start_date::timestamp with time zone, 'DD.MM.YYYY'::text) AS auct_date,
            rn_orders_hcs_determ_v.auction_price_min,
            rn_orders_hcs_determ_v.auction_price_fin,
            rn_orders_hcs_determ_v.area_km,
            rn_orders_hcs_determ_v.fields,
            rn_orders_hcs_determ_v.structures,
            rn_orders_hcs_determ_v.resource_type,
            rn_orders_hcs_determ_v.oil_ab1c1,
            rn_orders_hcs_determ_v.oil_b2c2,
            rn_orders_hcs_determ_v.oil_d0,
            rn_orders_hcs_determ_v.oil_dl,
            rn_orders_hcs_determ_v.oil_d1,
            rn_orders_hcs_determ_v.oil_d2,
            rn_orders_hcs_determ_v.gas_ab1c1,
            rn_orders_hcs_determ_v.gas_b2c2,
            rn_orders_hcs_determ_v.gas_d0,
            rn_orders_hcs_determ_v.gas_dl,
            rn_orders_hcs_determ_v.gas_d1,
            rn_orders_hcs_determ_v.gas_d2,
            'Определение победителя'::text AS status,
            rn_orders_hcs_determ_v.lot_url
           FROM rosnedra.rn_orders_hcs_determ_v
          WHERE rn_orders_hcs_determ_v.usage_type ~* 'разведка.*добыча'::text
        )
 SELECT rn_orders_nr_ne_report.order_n_date AS "Приказ №, дата",
    rn_orders_nr_ne_report.name AS "Наименование участка недр",

    rn_orders_nr_ne_report.subject_rf AS "Субъект РФ",
    rn_orders_nr_ne_report.planned_time AS "Планируемые сроки проведения аукц",
    rn_orders_nr_ne_report.organizer AS "Организатор",
    rn_orders_nr_ne_report.type_of_usage AS "Вид использования",
    rn_orders_nr_ne_report.lend_type AS "Способ распределения",
    rn_orders_nr_ne_report.appl_start_date AS "Дата начала подачи заявки",
    rn_orders_nr_ne_report.appl_end_date AS "Дата окончания подачи заявки",
    rn_orders_nr_ne_report.auct_date AS "Дата проведения торгов",
    rn_orders_nr_ne_report.auction_price_min AS "Начальная цена, руб (без НДС)",
    rn_orders_nr_ne_report.auction_price_fin AS "Итоговая цена, руб (без НДС)",
    rn_orders_nr_ne_report.area_km AS "S, кв.км",
    rn_orders_nr_ne_report.fields AS "Месторождение",
    rn_orders_nr_ne_report.structures AS "Подготовленные структуры",
    rn_orders_nr_ne_report.resource_type AS "тип УВ",
    rn_orders_nr_ne_report.oil_ab1c1 AS "нефть ab1c1, млн.т",
    rn_orders_nr_ne_report.oil_b2c2 AS "нефть b2c2, млн.т",
    rn_orders_nr_ne_report.oil_d0 AS "нефть d0, млн.т",
    rn_orders_nr_ne_report.oil_dl AS "нефть dl, млн.т",
    rn_orders_nr_ne_report.oil_d1 AS "нефть d1, млн.т",
    rn_orders_nr_ne_report.oil_d2 AS "нефть d2, млн.т",
    rn_orders_nr_ne_report.gas_ab1c1 AS "газ ab1c1, млрд.м3",
    rn_orders_nr_ne_report.gas_b2c2 AS "газ b2c2, млрд.м3",
    rn_orders_nr_ne_report.gas_d0 AS "газ d0, млрд.м3",
    rn_orders_nr_ne_report.gas_dl AS "газ dl, млрд.м3",
    rn_orders_nr_ne_report.gas_d1 AS "газ d1, млрд.м3",
    rn_orders_nr_ne_report.gas_d2 AS "газ d2, млрд.м3",
    rn_orders_nr_ne_report.status AS "Статус",
    rn_orders_nr_ne_report.lot_url AS "Ссылка на карточку лота"
   FROM rn_orders_nr_ne_report
  ORDER BY ((regexp_match(rn_orders_nr_ne_report.order_n_date, '(\d{1,3}) от'::text))[1]::numeric), rn_orders_nr_ne_report.name;




CREATE OR REPLACE VIEW rosnedra.rosnedra_orders_short
 AS
 SELECT row_number() OVER () AS gid,
    regexp_replace(split_part(rn.name::text, chr(10), 1), '[\r\n]'::text, ' '::text, 'g'::text) AS name,
    regexp_replace(rn.regions::text, '[\r\n]'::text, ' '::text, 'g'::text) AS regions,
    rn.area_km,
    regexp_replace(regexp_replace(regexp_replace(array_to_string(regexp_match(rn.name::text, '.есторождени.:\n([^\n]+)\).?\n|.есторождени.:\n([^\n]+) *.?\n|.есторождени.: *(.+)\)'::text), ''::text), '\s{2,}'::text, ' '::text, 'g'::text), '\n'::text, ''::text, 'g'::text), '[\r\n]'::text, ' '::text, 'g'::text) AS fields,
    regexp_replace(array_to_string(regexp_split_to_array(regexp_replace(regexp_replace(array_to_string(regexp_match(rn.name::text, '.труктур.:(.+)\).? *\n|.труктур.: *\n([^\n]+)\n|.труктур.: *\n([^\n]+)\)|\((.+) структура\)|.труктур.: *([^\n]+)\n'::text), ''::text), '\n'::text, ''::text, 'g'::text), ' {2,}'::text, ' '::text, 'g'::text), ', *'::text), ', '::text), '[\r\n]'::text, ' '::text, 'g'::text) AS structures,
    regexp_replace(regexp_replace(regexp_replace(rn.resource_type::text, '[\r\n]'::text, ' '::text, 'g'::text), ' {2,}'::text, ' '::text, 'g'::text), ' (?=,)'::text, ''::text, 'g'::text) AS resource_type,
        CASE
            WHEN (regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}'::text))[1] = ANY (ARRAY['1'::text, '2'::text, '3'::text, '4'::text]) THEN NULL::text
            ELSE (regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}'::text))[1]
        END AS planned_time,
    regexp_replace((regexp_split_to_array(rn.planned_terms_conditions::text, '[\r\n]|\s{3,}'::text))[2], '[\r\n]'::text, ' '::text, 'g'::text) AS organizer,
    rn.appl_deadline,
    regexp_replace(rn.usage_type::text, '[\r\n]'::text, ' '::text, 'g'::text) AS usage_type,
        CASE
            WHEN rn.lend_type::text = ANY (ARRAY['1'::character varying::text, '2'::character varying::text, '3'::character varying::text, '4'::character varying::text, '5'::character varying::text, '6'::character varying::text, 'nan'::character varying::text]) THEN NULL::character varying
            ELSE rn.lend_type
        END AS lend_type,
        CASE
            WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C1"[0]'::jsonpath)::numeric, 0::numeric)
            ELSE NULL::numeric
        END AS oil_ab1c1,
        CASE
            WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."C2"[0]'::jsonpath)::numeric, 0::numeric)
            ELSE NULL::numeric
        END AS oil_b2c2,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D0"[0]'::jsonpath)::numeric AS oil_d0,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."Dл"[0]'::jsonpath)::numeric AS oil_dl,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D1"[0]'::jsonpath)::numeric AS oil_d1,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."oil"."D2"[0]'::jsonpath)::numeric AS oil_d2,
        CASE

            WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C1"[0]'::jsonpath)::numeric, 0::numeric)
            ELSE NULL::numeric
        END AS gas_ab1c1,
        CASE
            WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."C2"[0]'::jsonpath)::numeric, 0::numeric)
            ELSE NULL::numeric
        END AS gas_b2c2,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D0"[0]'::jsonpath)::numeric AS gas_d0,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."Dл"[0]'::jsonpath)::numeric AS gas_dl,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D1"[0]'::jsonpath)::numeric AS gas_d1,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."gas"."D2"[0]'::jsonpath)::numeric AS gas_d2,
        CASE
            WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."A"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B1"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C1"[0]'::jsonpath)::numeric, 0::numeric)
            ELSE NULL::numeric
        END AS cond_ab1c1,
        CASE
            WHEN (COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric, 0::numeric)) > 0::numeric THEN COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."B2"[0]'::jsonpath)::numeric, 0::numeric) + COALESCE(jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."C2"[0]'::jsonpath)::numeric, 0::numeric)
            ELSE NULL::numeric
        END AS cond_b2c2,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D0"[0]'::jsonpath)::numeric AS cond_d0,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."Dл"[0]'::jsonpath)::numeric AS cond_dl,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D1"[0]'::jsonpath)::numeric AS cond_d1,
    jsonb_path_query_first(rn.resources_parsed::jsonb, '$."cond"."D2"[0]'::jsonpath)::numeric AS cond_d2,
    rn.source_url AS order_url,
    rn.source_name AS order_name,
    rn.order_date,
    rn.appl_deadline - CURRENT_DATE AS days_to_deadline,
        CASE
            WHEN lc.gid IS NOT NULL THEN true
            ELSE false
        END AS has_lot,
    lc."lotStatus" AS auction_lot_status,
        CASE
            WHEN lc."priceMin" IS NULL THEN ''::text
            ELSE concat(TRIM(BOTH FROM to_char(lc."priceMin", '999 999 999 999D99'::text)), ' ', chr(8381))
        END AS auction_price_min,
        CASE
            WHEN lc."priceFin" IS NULL THEN ''::text

            ELSE concat(TRIM(BOTH FROM to_char(lc."priceFin", '999 999 999 999D99'::text)), ' ', chr(8381))
        END AS auction_price_fin,
    ('https://torgi.gov.ru/new/public/lots/lot/'::text || lc.id::text) || '/(lotInfo:info)?fromRec=false'::text AS lot_url,
    (((lc."createDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_publish_datetime,
    (((lc."biddEndTime" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_appl_end_datetime,
    (((lc."auctionStartDate" + '00:01:00'::interval * lc."timeZoneOffset"::double precision)::text) || ' '::text) || lc."timeZoneName"::text AS auct_start_datetime,
    lc."createDate"::date AS auct_create_date,
    lc."biddEndTime"::date AS auct_appl_end_date,
    lc."auctionStartDate"::date AS auct_start_date,
    CURRENT_DATE - lc."auctionStartDate"::date AS days_from_auct,
    regexp_replace(rn.rfgf_gos_reg_num::text, '[\r\n]'::text, ' '::text, 'g'::text) AS rfgf_gos_reg_num,
    rfgf.user_info AS rfgf_license_user,
    rfgf.date_register AS rfgf_date_register,
    rfgf.date_license_stop AS rfgf_date_stop,
    rfgf.rfgf_link AS rfgf_url,
    rn.geom
   FROM rosnedra.license_blocks_rosnedra_orders rn
     LEFT JOIN torgi_gov_ru.lotcards lc ON lc.rn_guid::text = rn.rn_guid::text
     LEFT JOIN rfgf.license_blocks_rfgf rfgf ON rfgf.gos_reg_num::text = rn.rfgf_gos_reg_num::text
  WHERE date_part('year'::text, rn.order_date) >= (date_part('year'::text, CURRENT_DATE) - 1::double precision) AND (lower(rn.resource_type::text) ~~ '%нефть%'::text OR lower(rn.resource_type::text) ~~ '%газ%'::text OR lower(rn.resource_type::text) ~~ '%конденсат%'::text OR rn.resource_type::text = '1'::text) AND (lc."createDate" IS NULL OR date_part('year'::text, lc."createDate") >= (date_part('year'::text, CURRENT_DATE) - 1::double precision) AND lc."createDate" >= rn.order_date);




CREATE OR REPLACE FUNCTION rosnedra.get_rfgf_gos_reg_num(
  _rn_guid text)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare rosnedra_block_row RECORD;
declare rfgf_row RECORD;
declare _result text;
begin
  _result := NULL;
  FOR rosnedra_block_row IN
    select * from (select v.gid, v.name, v.rn_guid, st_makevalid(v.geom) as geom from rosnedra.license_blocks_rosnedra_orders v) t where t.rn_guid = _rn_guid and st_isvalid(t.geom)
  loop
    for rfgf_row in
      select * from rfgf.license_blocks_rfgf r 
      where r.license_cancel_order_info IS NULL AND (r.date_license_stop IS NULL OR r.date_license_stop >= CURRENT_DATE)
      and st_isvalid(r.geom) 
      and st_intersects(r.geom, rosnedra_block_row.geom) 
      and st_area(st_makevalid(st_symdifference(r.geom, rosnedra_block_row.geom))) / st_area(st_makevalid(rosnedra_block_row.geom)) <= 0.1
      --and abs((st_area(st_makevalid(st_intersection(r.geom, rosnedra_block_row.geom))) / st_area(rosnedra_block_row.geom)) - 1) <= 0.1
      --and st_area(st_makevalid(st_intersection(r.geom, rosnedra_block_row.geom))) >= 0.9 * st_area(rosnedra_block_row.geom) 
      --and LOWER(rosnedra_block_row.name) ~ TRIM(REPLACE(REPLACE(LOWER(r.license_block_name), 'участок', ''), 'месторождение', ''))
    loop
      if rfgf_row.id IS NOT NULL then
        _result := rfgf_row.gos_reg_num;
      end if;
    end loop;
  end loop;
  return _result;
end;
$BODY$;



CREATE OR REPLACE FUNCTION torgi_gov_ru.lotcard_get_rn_guid(
  _gid integer)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare lotcard_row RECORD;
declare rosnedra_row RECORD;
declare _result text;
begin
  _result := '';
  FOR lotcard_row IN
    select * from torgi_gov_ru.lotcards t where t.gid = _gid
  loop
    for rosnedra_row in
      select * from rosnedra.license_blocks_rosnedra_orders r 
      where
      TRIM(BOTH FROM replace((string_to_array(lower(r.name::text), chr(10)))[1], 'участок'::text, ''::text)) = 
      TRIM(BOTH FROM replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(lower(lotcard_row."miningSiteName_EA(N)"::text), 'участок недрор'::text, ''::text), 'участок недрссс'::text, ''::text), 'участок недр'::text, ''::text), 'участок ндр'::text, ''::text), 'участке недр.п'::text, ''::text), 'архангельское месторождение'::text, 'архангельский (месторождение архангельское)'::text), '(месторождения: падимейское)'::text, ''::text), 'в пермском крае'::text, ''::text), 'лободинское месторождение (часть)'::text, 'лободинский'::text), 'участок'::text, ''::text), 'месторождение'::text, ''::text), '(месторождения: Кодачское, подготовленные структуры: Восточно-Кодачская)"  "Республика Коми"'::text, ''::text), chr(10), ''::text), chr(34), ''::text)) 
      AND 
      ((string_to_array(lotcard_row."resourceLocation_EA(N)"::text, ', '::text))[1] = ANY (string_to_array(r.regions::text, ', '::text)))
    loop
      if rosnedra_row.rn_guid IS NOT NULL then
        _result := rosnedra_row.rn_guid;
      end if;
    end loop;
  end loop;
  return _result;
end;
$BODY$;




CREATE OR REPLACE FUNCTION torgi_gov_ru.lotcard_get_rosnedra_gid(
  _gid integer)
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare lotcard_row RECORD;
declare rosnedra_row RECORD;
declare _result integer;
begin
  _result := 0;
  FOR lotcard_row IN
    select * from torgi_gov_ru.lotcards t where t.gid = _gid
  loop
    for rosnedra_row in
      select * from rosnedra.license_blocks_rosnedra_orders r 
      where
      TRIM(BOTH FROM replace((string_to_array(lower(r.name::text), chr(10)))[1], 'участок'::text, ''::text)) = 
      TRIM(BOTH FROM replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(lower(lotcard_row."miningSiteName_EA(N)"::text), 'участок недрор'::text, ''::text), 'участок недрссс'::text, ''::text), 'участок недр'::text, ''::text), 'участок ндр'::text, ''::text), 'участке недр.п'::text, ''::text), 'архангельское месторождение'::text, 'архангельский (месторождение архангельское)'::text), '(месторождения: падимейское)'::text, ''::text), 'в пермском крае'::text, ''::text), 'лободинское месторождение (часть)'::text, 'лободинский'::text), 'участок'::text, ''::text), 'месторождение'::text, ''::text), '(месторождения: Кодачское, подготовленные структуры: Восточно-Кодачская)"  "Республика Коми"'::text, ''::text), chr(10), ''::text), chr(34), ''::text)) 
      AND 
      ((string_to_array(lotcard_row."resourceLocation_EA(N)"::text, ', '::text))[1] = ANY (string_to_array(r.regions::text, ', '::text)))
    loop
      if rosnedra_row.gid IS NOT NULL then
        _result := rosnedra_row.gid;
      end if;
    end loop;
  end loop;
  return _result;
end;
$BODY$;

CREATE INDEX IF NOT EXISTS license_blocks_rfgf_geom_gist
            ON rfgf.license_blocks_rfgf
            USING GIST (geom);
            ANALYZE rfgf.license_blocks_rfgf;

CREATE INDEX IF NOT EXISTS rosnedra_orders_geom_gist
            ON rosnedra.license_blocks_rosnedra_orders
            USING GIST (geom);
            ANALYZE rosnedra.license_blocks_rosnedra_orders;