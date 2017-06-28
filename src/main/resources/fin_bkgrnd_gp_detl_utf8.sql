set mapred.job.name = fin_bkgrnd_gp_detl;

set hive.map.aggr = true;

drop table {$ORA_TMP}.tmp_fin_bkgrnd_gp_detl;

create table {$ORA_TMP}.tmp_fin_bkgrnd_gp_detl (     DATE_ID string comment '日期',
    MTH_ID string comment '月份',
    PROD_ID bigint comment '产品ID',
    SUPLR_ID bigint comment '供应商ID',
    cntrt_code  string comment '',
    DISTRI_METHD bigint comment '经销方式 1:经销 2:SBY 3.代销 4.DSV',
    PRCHS_CNTRT_RBTE_AMT double comment '合同返利额(采购返利)',
    SALE_CNTRT_RBTE_AMT double comment '合同返利(销售返利)',
    EARLY_STOCK_AMT double comment '期初库存金额',
    FINAL_STOCK_AMT double comment '期末库存金额',
    HTFL_CPSH_KD double comment '合同返利-产品损耗扣点',
    HTFL_MBXXSZK_KD double comment '合同返利-目标性销售折扣扣点',
    HTFL_WLBTA_KD double comment '合同返利-物流补贴A扣点',
    HTFL_WLBTB_KD double comment '合同返利-物流补贴B扣点',
    no_tax_early_stock_amt  double comment '未税合同返利额(采购返利)',
    no_tax_final_stock_amt  double comment '未税合同返利(销售返利)',
    stock_amt_d  double comment '',
    no_tax_sale_cntrt_rbte_amt  double comment '',
    no_tax_stock_amt_d  double comment '',
    no_tax_stock_amt_d_pre  double comment '',
    no_tax_prchs_cntrt_rbte_amt  double comment '') comment '财务后台毛利明细表' stored as parquet;
    

insert overwrite table {$ORA_TMP}.tmp_fin_bkgrnd_gp_detl
  select case when t1.date_id is null then  t2.date_id else t1.date_id end AS date_id,
         case when t1.mth_id is null then  t2.mth_id else t1.mth_id end AS mth_id,
         case when t1.product_id is null then  t2.product_id else t1.product_id end AS prod_id,
         case when t1.supplier_id is null then t2.supplier_id else t1.supplier_id end AS suplr_id,
         case when t1.cntrt_code is null then  t2.cntrt_code else t1.cntrt_code end AS cntrt_code,
         case when t1.distri_methd is null then  t2.distri_methd else t1.distri_methd end AS distri_methd,
         SUM(prchs_cntrt_rbte_amt) as prchs_cntrt_rbte_amt,
         SUM(sale_cntrt_rbte_amt) as sale_cntrt_rbte_amt,
         SUM(early_stock_amt) as early_stock_amt,
         SUM(final_stock_amt) as final_stock_amt,
         SUM(htfl_cpsh_kd) as htfl_cpsh_kd,
         SUM(htfl_mbxxszk_kd) as htfl_mbxxszk_kd,
         SUM(htfl_wlbta_kd) as htfl_wlbta_kd,
         SUM(htfl_wlbtb_kd) as htfl_wlbtb_kd,
         SUM(no_tax_early_stock_amt) as no_tax_early_stock_amt,
         SUM(no_tax_final_stock_amt) as no_tax_final_stock_amt,
         SUM(stock_amt_d) as stock_amt_d,
         SUM(no_tax_sale_cntrt_rbte_amt) as no_tax_sale_cntrt_rbte_amt,
         SUM(no_tax_stock_amt_d) as no_tax_stock_amt_d,
         SUM(no_tax_stock_amt_d_pre) as no_tax_stock_amt_d_pre,
         SUM(no_tax_prchs_cntrt_rbte_amt) as no_tax_prchs_cntrt_rbte_amt
    FROM (SELECT to_date(a.record_date) AS date_id,
                 concat(a.record_year_month, '-01') AS mth_id,
                 a.product_id,
                 b.supplier_id,
                 cn.cntrt_code,
                 cn.distri_methd,
                 SUM(COALESCE(product_rebate_account, 0)) AS prchs_cntrt_rbte_amt,
                 SUM(COALESCE(product_rebate_account / (1 + COALESCE(tax_rate, 0)), 0)) AS no_tax_prchs_cntrt_rbte_amt
            FROM {$ORA_ODS}.cntrt_rebate_record_det_d a
           INNER JOIN {$ORA_ODS}.contract_rebate_info b
              ON a.rebate_info_id = b.id
            LEFT JOIN {$ORA_DW}.sc_cntrt cn
              ON b.contract_code = cn.cntrt_code
             AND cn.cntrt_latst_flg = 1
          --AND    cn.cur_flag = 1
            LEFT JOIN {$ORA_DW}.pm_prod p
              ON a.product_id = p.prod_id
             AND p.cur_flg = 1
           WHERE to_date(a.record_date) >= '{$DATE_ID}'
             AND to_date(a.record_date) < '{$TX_DATE}'
             AND product_id IS NOT NULL
           GROUP BY to_date(a.record_date),
                    concat(a.record_year_month, '-01'),
                    a.product_id,
                    b.supplier_id,
                    cn.distri_methd,
                    cn.cntrt_code) t1
    FULL OUTER JOIN (SELECT '{$DATE_ID}' AS date_id,
                            '{$MonthFD_0}' AS mth_id,
                            case when a.product_id is null then  bkg.prod_id else a.product_id end AS product_id,
                            case when a.supplier_id is null then  bkg.suplr_id else a.supplier_id end AS supplier_id,
                            case when a.distri_methd is null then  bkg.distri_methd else a.distri_methd end AS distri_methd,
                            case when bkg.final_stock_amt is null then  0 else bkg.final_stock_amt end AS early_stock_amt,
                            COALESCE(a.final_stock_amt, 0) AS final_stock_amt,
                            case when cn.cntrt_code is null then  bkg.cntrt_code else cn.cntrt_code end AS cntrt_code,
                            COALESCE(rc.goods_wastage_rate, 0) AS htfl_cpsh_kd,
                            COALESCE(rc.award_rate, 0) AS htfl_mbxxszk_kd,
                            COALESCE(rc.subsidy_b, 0) AS htfl_wlbta_kd,
                            COALESCE(rc.subsidy, 0) AS htfl_wlbtb_kd,
                            COALESCE(bkg.stock_amt_d, 0) - COALESCE(a.final_stock_amt, 0) * (COALESCE(rc.goods_wastage_rate, 0) +COALESCE(rc.award_rate, 0) + COALESCE(rc.subsidy_b, 0) + COALESCE(rc.subsidy, 0) + COALESCE(rgv5.subsidy_percent, 0)) / 100 AS sale_cntrt_rbte_amt,
                            COALESCE(bkg.no_tax_final_stock_amt, 0) AS no_tax_early_stock_amt,
                            COALESCE(a.no_tax_final_stock_amt, 0) AS no_tax_final_stock_amt,
                            COALESCE(a.final_stock_amt, 0) *(COALESCE(rc.goods_wastage_rate, 0) + COALESCE(rc.award_rate, 0) + COALESCE(rc.subsidy_b, 0) + COALESCE(rc.subsidy, 0) + COALESCE(rgv5.subsidy_percent, 0)) / 100 AS stock_amt_d,
                            COALESCE(bkg.no_tax_stock_amt_d, 0) - COALESCE(a.no_tax_final_stock_amt, 0) * (COALESCE(rc.goods_wastage_rate, 0) + COALESCE(rc.award_rate, 0) + COALESCE(rc.subsidy_b, 0) + COALESCE(rc.subsidy, 0) + COALESCE(rgv5.subsidy_percent, 0)) / 100 AS no_tax_sale_cntrt_rbte_amt,
                            COALESCE(a.no_tax_final_stock_amt, 0) *(COALESCE(rc.goods_wastage_rate, 0) + COALESCE(rc.award_rate, 0) + COALESCE(rc.subsidy_b, 0) + COALESCE(rc.subsidy, 0) + COALESCE(rgv5.subsidy_percent, 0)) / 100 AS no_tax_stock_amt_d,
                            COALESCE(bkg.no_tax_stock_amt_d, 0) AS no_tax_stock_amt_d_pre
                       FROM (SELECT pd.prod_id AS product_id,
                                    pd.suplr_id AS supplier_id,
                                    pd.distri_methd,
                                    SUM(ap.distri_mv_avg_price * (COALESCE(stock_num, 0) + COALESCE(damag_stock_num, 0))) final_stock_amt,
                                    SUM(ap.no_tax_distri_mv_avg_price * (COALESCE(stock_num, 0) + COALESCE(damag_stock_num, 0))) no_tax_final_stock_amt
                               FROM {$ORA_DW}.sc_invtry_age pd
                              INNER JOIN {$ORA_DW}.pm_batch_mv_avg_price ap
                                 ON pd.prod_id = ap.prod_id
                                AND pd.merchant_group_id = ap.mct_group_id
                                AND to_date(pd.date_id) = to_date(ap.date_id)
                              WHERE to_date(pd.date_id) = '{$DATE_ID}'
                                AND to_date(ap.date_id) = '{$DATE_ID}'
                                AND pd.distri_methd = 1
                                AND pd.merchant_group_id <> 200
                              GROUP BY pd.prod_id,
                                       pd.suplr_id,
                                       pd.distri_methd) a
                       LEFT JOIN {$ORA_DW}.sc_cntrt cn
                         ON a.supplier_id = cn.suplr_id
                        AND cn.cntrt_curr_sttus = 2
                        AND cn.cntrt_latst_flg = 1
                        AND cn.agrmt_type = 1
                        AND cn.distri_methd = 1
                       LEFT JOIN {$ORA_ODS}.rebateagreement_clause rc
                         ON cn.cntrt_id = rc.contract_id
                        AND to_date(rc.date_id) = '{$DATE_ID}'
                       LEFT JOIN {$ORA_DW}.sc_contract_return_goods_v5 rgv5
                         ON cn.cntrt_id = rgv5.contract_id
                        AND to_date(rgv5.date_id) = '{$DATE_ID}'
                       LEFT JOIN {$ORA_DW}.std_cntrt_rbte_rate t2
                         ON cn.cntrt_code = t2.cntrt_code
                       FULL OUTER JOIN (SELECT *
                                         FROM {$ORA_DW}.fin_bkgrnd_gp_detl
                                        -- FROM {$ORA_DW}.fin_bkgrnd_gp_detl
                                        WHERE distri_methd = 1
                                          AND final_stock_amt <> 0
                                          AND to_date(date_id) = date_sub('{$DATE_ID}', 1)) bkg
                         ON a.product_id = bkg.prod_id
                        AND a.supplier_id = bkg.suplr_id
                     ) t2
      ON t1.date_id = t2.date_id
     AND t1.mth_id = t2.mth_id
     AND t1.product_id = t2.product_id
     AND t1.supplier_id = t2.supplier_id
     AND t1.distri_methd = t2.distri_methd
     AND t1.cntrt_code = t2.cntrt_code
   GROUP BY case when t1.date_id is null then  t2.date_id else t1.date_id end,
            case when t1.mth_id is null then  t2.mth_id  else t1.mth_id end,
            case when t1.product_id is null then  t2.product_id  else t1.product_id end,
            case when t1.supplier_id is null then  t2.supplier_id else t1.supplier_id end,
            case when t1.cntrt_code is null then  t2.cntrt_code else t1.cntrt_code end,
            case when t1.distri_methd is null then  t2.distri_methd else t1.distri_methd end;

  
  insert overwrite table {$ORA_DW}.fin_bkgrnd_gp_detl
    select date_id,
           mth_id,
           prod_id,
           prod_code,
           suplr_id,
           suplr_code,
           cntrt_id,
           cntrt_code,
           distri_methd,
           tax_rate,
           prchs_cntrt_rbte_amt,
           sale_cntrt_rbte_amt,
           early_stock_amt,
           final_stock_amt,
           htfl_cpsh_kd,
           htfl_mbxxszk_kd,
           htfl_wlbta_kd,
           htfl_wlbtb_kd,
           no_tax_prchs_cntrt_rbte_amt,
           no_tax_sale_cntrt_rbte_amt,
           pk_dis_rbte_amt,
           pk_dis_rbte_rate,
           fpk_dis_rbte_amt,
           fpk_dis_rbte_rate,
           no_tax_pk_dis_rbte_amt,
           no_tax_pk_dis_rbte_rate,
           no_tax_fpk_dis_rbte_amt,
           no_tax_fpk_dis_rbte_rate,
           jvc_ad_incom_amt,
           no_tax_jvc_ad_incom_amt,
           bkgrnd_gp_amt,
           no_tax_bkgrnd_gp_amt,
           etl_time,
           no_tax_early_stock_amt,
           no_tax_final_stock_amt,
           stock_amt_d,
           no_tax_stock_amt_d
      from {$ORA_DW}.fin_bkgrnd_gp_detl
     where to_date(date_id) < '{$DATE_ID}'
    union all
    select tmp.date_id,
           tmp.mth_id,
           tmp.prod_id,
           p.prod_code,
           tmp.suplr_id,
           s.suplr_code,
           cn.cntrt_id,
           tmp.cntrt_code,
           tmp.distri_methd,
           COALESCE(p.sale_tax_rate, 0.17) AS sale_tax_rate,
           COALESCE(tmp.prchs_cntrt_rbte_amt, 0),
           COALESCE(tmp.sale_cntrt_rbte_amt, 0) AS sale_cntrt_rbte_amt,
           COALESCE(tmp.early_stock_amt, 0),
           COALESCE(tmp.final_stock_amt, 0),
           COALESCE(tmp.htfl_cpsh_kd, 0),
           COALESCE(tmp.htfl_mbxxszk_kd, 0),
           COALESCE(tmp.htfl_wlbta_kd, 0),
           COALESCE(tmp.htfl_wlbtb_kd, 0),
           COALESCE(tmp.no_tax_prchs_cntrt_rbte_amt, 0) AS no_tax_prchs_cntrt_rbte_amt,
           COALESCE(tmp.no_tax_sale_cntrt_rbte_amt, 0) AS no_tax_sale_cntrt_rbte_amt,
           COALESCE(tmp.pk_dis_rbte_amt, 0) AS pk_dis_rbte_amt,
           COALESCE(tmp.pk_dis_rbte_rate, 0) AS pk_dis_rbte_rate,
           COALESCE(tmp.fpk_dis_rbte_amt, 0) AS fpk_dis_rbte_amt,
           COALESCE(tmp.fpk_dis_rbte_rate, 0) AS fpk_dis_rbte_rate,
           COALESCE(tmp.no_tax_pk_dis_rbte_amt, 0) AS no_tax_pk_dis_rbte_amt,
           COALESCE(tmp.no_tax_pk_dis_rbte_rate, 0) AS no_tax_pk_dis_rbte_rate,
           COALESCE(tmp.no_tax_fpk_dis_rbte_amt, 0) AS no_tax_fpk_dis_rbte_amt,
           COALESCE(tmp.no_tax_fpk_dis_rbte_rate, 0) AS no_tax_fpk_dis_rbte_rate,
           COALESCE(tmp.jvc_ad_incom_amt, 0) AS jvc_ad_incom_amt,
           COALESCE(tmp.no_tax_jvc_ad_incom_amt, 0) AS no_tax_jvc_ad_incom_amt,
           COALESCE(tmp.prchs_cntrt_rbte_amt, 0) + COALESCE(tmp.sale_cntrt_rbte_amt, 0) + COALESCE(tmp.pk_dis_rbte_amt, 0) + COALESCE(tmp.pk_dis_rbte_rate, 0) + COALESCE(tmp.fpk_dis_rbte_amt, 0) + COALESCE(tmp.fpk_dis_rbte_rate, 0) + COALESCE(tmp.jvc_ad_incom_amt, 0) AS bkgrnd_gp_amt,
           COALESCE(tmp.no_tax_prchs_cntrt_rbte_amt, 0) +COALESCE(tmp.no_tax_sale_cntrt_rbte_amt, 0) +  COALESCE(tmp.no_tax_pk_dis_rbte_amt, 0) + COALESCE(tmp.no_tax_pk_dis_rbte_rate, 0) + COALESCE(tmp.no_tax_fpk_dis_rbte_amt, 0) + COALESCE(tmp.no_tax_fpk_dis_rbte_rate, 0) + COALESCE(tmp.no_tax_jvc_ad_incom_amt, 0) + 0 AS no_tax_bkgrnd_gp_amt,
           getdatetime(),
           COALESCE(tmp.no_tax_early_stock_amt, 0),
           COALESCE(tmp.no_tax_final_stock_amt, 0),
           COALESCE(tmp.stock_amt_d, 0),
           COALESCE(tmp.no_tax_stock_amt_d, 0)
      FROM (SELECT t1.date_id AS date_id,
                   t1.mth_id AS mth_id,
                   t1.prod_id AS prod_id,
                   t1.suplr_id AS suplr_id,
                   t1.cntrt_code AS cntrt_code,
                   t1.distri_methd AS distri_methd,
                   COALESCE(t1.prchs_cntrt_rbte_amt, 0) AS prchs_cntrt_rbte_amt,
                   COALESCE(t1.no_tax_prchs_cntrt_rbte_amt, 0) AS no_tax_prchs_cntrt_rbte_amt,
                   COALESCE(t1.sale_cntrt_rbte_amt, 0) AS sale_cntrt_rbte_amt,
                   COALESCE(t1.no_tax_sale_cntrt_rbte_amt, 0) AS no_tax_sale_cntrt_rbte_amt,
                   COALESCE(t1.early_stock_amt, 0) AS early_stock_amt,
                   COALESCE(t1.final_stock_amt, 0) AS final_stock_amt,
                   COALESCE(t1.htfl_cpsh_kd, 0) AS htfl_cpsh_kd,
                   COALESCE(t1.htfl_mbxxszk_kd, 0) AS htfl_mbxxszk_kd,
                   COALESCE(t1.htfl_wlbta_kd, 0) AS htfl_wlbta_kd,
                   COALESCE(t1.htfl_wlbtb_kd, 0) AS htfl_wlbtb_kd,
                   0 AS pk_dis_rbte_amt,
                   0 AS no_tax_pk_dis_rbte_amt,
                   0 AS pk_dis_rbte_rate,
                   0 AS no_tax_pk_dis_rbte_rate,
                   0 AS fpk_dis_rbte_amt,
                   0 AS no_tax_fpk_dis_rbte_amt,
                   0 AS fpk_dis_rbte_rate,
                   0 AS no_tax_fpk_dis_rbte_rate,
                   0 AS jvc_ad_incom_amt,
                   0 AS no_tax_jvc_ad_incom_amt,
                   no_tax_early_stock_amt,
                   no_tax_final_stock_amt,
                   stock_amt_d,
                   no_tax_stock_amt_d
              FROM {$ORA_DW}.fin_bkgrnd_gp_detl t1
            UNION ALL
            SELECT to_date(t1.create_date) AS date_id,
                   concat(t1.record_year_month, '-01') AS mth_id,
                   COALESCE(t1.product_id, p.prod_id),
                   t1.supplier_id,
                   t2.contract_code,
                   cn.distri_methd,
                   0 AS prchs_cntrt_rbte_amt,
                   0 AS no_tax_prchs_cntrt_rbte_amt,
                   0 AS sale_cntrt_rbte_amt,
                   0 AS no_tax_sale_cntrt_rbte_amt,
                   0 AS early_stock_amt,
                   0 AS final_stock_amt,
                   0 AS htfl_cpsh_kd,
                   0 AS htfl_mbxxszk_kd,
                   0 AS htfl_wlbta_kd,
                   0 AS htfl_wlbtb_kd,
                   SUM(CASE
                         WHEN t2.pay_type = 1 AND t2.return_commission_type = 5 THEN
                          COALESCE(record_rebate, 0)
                         ELSE
                          0
                       END) pk_dis_rbte_amt,
                   SUM(CASE
                         WHEN t2.pay_type = 1 AND t2.return_commission_type = 5 THEN
                          COALESCE(record_rebate, 0) / (1 + COALESCE(tax_rate, 0))
                         ELSE
                          0
                       END) no_tax_pk_dis_rbte_amt,
                   SUM(CASE
                         WHEN t2.pay_type = 1 AND t2.return_commission_type <> 5 THEN
                          COALESCE(record_rebate, 0)
                         ELSE
                          0
                       END) pk_dis_rbte_rate,
                   SUM(CASE
                         WHEN t2.pay_type = 1 AND t2.return_commission_type <> 5 THEN
                          COALESCE(record_rebate, 0) / (1 + COALESCE(tax_rate, 0))
                         ELSE
                          0
                       END) no_tax_pk_dis_rbte_rate,
                   SUM(CASE
                         WHEN t2.pay_type <> 1 AND t2.return_commission_type = 5 THEN
                          COALESCE(record_rebate, 0)
                         ELSE
                          0
                       END) fpk_dis_rbte_amt,
                   SUM(CASE
                         WHEN t2.pay_type <> 1 AND t2.return_commission_type = 5 THEN
                          COALESCE(record_rebate, 0) / (1 + COALESCE(tax_rate, 0))
                         ELSE
                          0
                       END) no_tax_fpk_dis_rbte_amt,
                   SUM(CASE
                         WHEN t2.pay_type <> 1 AND t2.return_commission_type <> 5 THEN
                          COALESCE(record_rebate, 0)
                         ELSE
                          0
                       END) fpk_dis_rbte_rate,
                   SUM(CASE
                         WHEN t2.pay_type <> 1 AND t2.return_commission_type <> 5 THEN
                          COALESCE(record_rebate, 0) / (1 + COALESCE(tax_rate, 0))
                         ELSE
                          0
                       END) no_tax_fpk_dis_rbte_rate,
                   0 jvc_ad_incom_amt,
                   0,
                   0,
                   0,
                   0,
                   0
              FROM {$ORA_ODS}.rebate_sku_dtl_d t1
              LEFT JOIN {$ORA_ODS}.rebate_agreement t2
                ON t1.rebate_code = t2.rebate_code
              LEFT JOIN (SELECT DISTINCT cntrt_code   as cntrt_code,
                                         distri_methd as distri_methd
                           FROM {$ORA_DW}.sc_cntrt) cn
                ON t2.contract_code = cn.cntrt_code
              LEFT JOIN (select t.prod_code as prod_code,t.cur_flg as cur_flg, t.prod_id as prod_id from {$ORA_DW}.pm_prod t where t.prod_code <> 'needdelete') p
                ON t1.product_code = p.prod_code
               AND p.cur_flg = 1
              -- AND p.prod_code <> 'needdelete'
             WHERE to_date(t1.create_date) >= '{$DATE_ID}'
               AND to_date(t1.create_date) < '{$TX_DATE}'
               AND t1.record_type <> 7
             GROUP BY to_date(t1.create_date),
                      concat(t1.record_year_month, '-01'),
                      COALESCE(t1.product_id, p.prod_id),
                      t1.supplier_id,
                      t2.contract_code,
                      cn.distri_methd
            UNION ALL
            SELECT to_date(t1.create_date) AS date_id,
                   concat(t1.record_year_month, '-01') AS mth_id,
                   COALESCE(t1.product_id, p.prod_id),
                   t1.supplier_id,
                   t2.contract_code,
                   cn.distri_methd,
                   0 AS prchs_cntrt_rbte_amt,
                   0 AS no_tax_prchs_cntrt_rbte_amt,
                   0 AS sale_cntrt_rbte_amt,
                   0 AS no_tax_sale_cntrt_rbte_amt,
                   0 AS early_stock_amt,
                   0 AS final_stock_amt,
                   0 AS htfl_cpsh_kd,
                   0 AS htfl_mbxxszk_kd,
                   0 AS htfl_wlbta_kd,
                   0 AS htfl_wlbtb_kd,
                   0 pk_dis_rbte_amt,
                   0,
                   0 pk_dis_rbte_rate,
                   0,
                   0 fpk_dis_rbte_amt,
                   0,
                   0 fpk_dis_rbte_rate,
                   0,
                   SUM(COALESCE(record_rebate, 0)) jvc_ad_incom_amt,
                   SUM(t1.record_rebate /(1 + case when m.collecting_company=101 then 0.06 else 0 end)),
                   0,
                   0,
                   0,
                   0
              FROM {$ORA_ODS}.rebate_sku_dtl_d t1
              LEFT JOIN {$ORA_ODS}.rebate_agreement t2
                ON t1.rebate_code = t2.rebate_code
              LEFT JOIN (SELECT DISTINCT cntrt_code, distri_methd
                           FROM {$ORA_DW}.sc_cntrt) cn
                ON t2.contract_code = cn.cntrt_code
              LEFT JOIN (select t.prod_code as prod_code, t.cur_flg as cur_flg,t.prod_id as prod_id from {$ORA_DW}.pm_prod t where t.prod_code <>'needdelete') p
                ON t1.product_code = p.prod_code
               AND p.cur_flg = 1
              -- AND p.prod_code <> 'needdelete'
              LEFT JOIN {$ORA_ODS}.fin_base_market_income m
                ON m.code = t1.record_code
             WHERE to_date(t1.create_date) >= '{$DATE_ID}'
               AND to_date(t1.create_date) < '{$TX_DATE}'
               AND t1.record_type = 7
             GROUP BY to_date(t1.create_date),
                      concat(t1.record_year_month, '-01'),
                      COALESCE(t1.product_id, p.prod_id),
                      t1.supplier_id,
                      t2.contract_code,
                      cn.distri_methd) tmp
      LEFT JOIN {$ORA_DW}.pm_prod p
        ON tmp.prod_id = p.prod_id
       AND p.cur_flg = 1
      LEFT JOIN {$ORA_DW}.sc_suplr s
        ON tmp.suplr_id = s.suplr_id
      LEFT JOIN {$ORA_DW}.sc_cntrt cn
        ON tmp.cntrt_code = cn.cntrt_code
       AND cn.cntrt_latst_flg = 1;