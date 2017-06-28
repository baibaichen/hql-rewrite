public class TestConstant {
  static public final String
    createHQLExternal = "CREATE EXTERNAL TABLE test.dim_mrchnt(\n" +
    "  mrchnt_skid bigint, \n" +
    "  mrchnt_id bigint, \n" +
    "  mrchnt_name string, \n" +
    "  mc_site_id bigint, \n" +
    "  biz_unit bigint, \n" +
    "  mrchnt_type bigint, \n" +
    "  mrchnt_co_name string, \n" +
    "  comsn_rate double, \n" +
    "  delet_flag int, \n" +
    "  start_date string, \n" +
    "  end_date string, \n" +
    "  ver_num bigint, \n" +
    "  cur_flag int, \n" +
    "  etl_batch_id bigint, \n" +
    "  updt_time string, \n" +
    "  mrchnt_opat_type int, \n" +
    "  mrchnt_city_id bigint)";

  static public final String
    createHQL = "CREATE TABLE test.dim_mrchnt(\n" +
    "  mrchnt_skid bigint, \n" +
    "  mrchnt_id bigint, \n" +
    "  mrchnt_name string, \n" +
    "  mc_site_id bigint, \n" +
    "  biz_unit bigint, \n" +
    "  mrchnt_type bigint, \n" +
    "  mrchnt_co_name string, \n" +
    "  comsn_rate double, \n" +
    "  delet_flag int, \n" +
    "  start_date string, \n" +
    "  end_date string, \n" +
    "  ver_num bigint, \n" +
    "  cur_flag int, \n" +
    "  etl_batch_id bigint, \n" +
    "  updt_time string, \n" +
    "  mrchnt_opat_type int, \n" +
    "  mrchnt_city_id bigint)";

  static public final String
    dropHQL = "drop table if exists\n" +
    " xx.trivial;\r\n" +
    "drop table if exists xx.xxttrr";

  static public final String
    insertHQL = "insert overwrite table xxxxx.rpt_trfc  partition(ds= '{$label}') select * from u_data";

  static public final String
    SQL_rpt_trfc_cms = "#use dw\n" +
    "drop table {$DB_TMP}.tmp_cms_ordr_tranx;\n" +
    "--set mapred.reduce.tasks=20;  \n" +
    "create table {$DB_TMP}.tmp_cms_ordr_tranx as\n" +
    "   SELECT t1.parnt_ordr_id,\n" +
    "       t1.parnt_ordr_code,\n" +
    "       t1.end_user_id,\n" +
    "       max(case\n" +
    "         when t1.ordr_tranx_activ_flg = 1 and t1.ordr_card_flg = 0 and\n" +
    "              (t2.pm_amt <> 0 OR (t2.pm_amt = 0 AND t2.tot_intgl_cnt <> 0))  and t3.biz_unit <> 2  then\n" +
    "          1\n" +
    "         else\n" +
    "          0\n" +
    "       end) as ordr_tranx_activ_flg,\n" +
    "       t2.prod_id,\n" +
    "       t3.biz_unit,\n" +
    "       SUM(t2.pm_amt) AS sale_amt,\n" +
    "       SUM(t2.pm_net_amt) AS actl_sale_amt,\n" +
    "       SUM(t2.pm_num) AS sale_num\n" +
    "FROM   {$DB_DW}.fct_ordr_tranx t1\n" +
    "INNER  JOIN {$DB_DW}.fct_ordr_tranx_detl t2\n" +
    "ON     t1.ordr_id = t2.ordr_id\n" +
    "AND    t1.DS = t2.DS\n" +
    "INNER  JOIN {$DB_DW}.dim_mrchnt t3\n" +
    "ON     t2.mrchnt_id = t3.mrchnt_id\n" +
    "AND    t3.cur_flag = 1\n" +
    "WHERE  t1.DS ='{$label}'\n" +
    "AND    t2.DS ='{$label}'\n" +
    "GROUP  BY t1.parnt_ordr_id,\n" +
    "          t1.parnt_ordr_code,\n" +
    "          t2.prod_id,\n" +
    "          t3.biz_unit,\n" +
    "          t1.end_user_id;" +
    "\n" +
    " \n" +
    "drop table {$DB_TMP}.tmp_cms_sessn;\n" +
    "--set mapred.reduce.tasks=30; \n" +
    "create table {$DB_TMP}.tmp_cms_sessn as\n" +
    "SELECT ds,\n" +
    "       prod_id,\n" +
    "       gu_id,\n" +
    "       sessn_id,\n" +
    "       chanl_id,\n" +
    "\tsessn_chanl_id,\n" +
    "       pltfm_id,\n" +
    "       nav_page_value,\n" +
    "       sessn_pv,\n" +
    "       internal_result_sum,\n" +
    "       nav_tracker_id,\n" +
    "\t   nav_button_position,\n" +
    "       nav_next_tracker_id,\n" +
    "       detl_tracker_id,\n" +
    "       detl_button_position,\n" +
    "       cart_tracker_id,\n" +
    "       city_id,\n" +
    "       biz_unit,\n" +
    "       ordr_code,\n" +
    "       gu_sec_flg\n" +
    "FROM   (SELECT DISTINCT t1.ds,\n" +
    "                        CASE\n" +
    "                           WHEN dcp.prod_item_id IS NULL\n" +
    "                                OR length(dcp.prod_item_id) = 0 THEN\n" +
    "                            t1.cart_prod_id\n" +
    "                           ELSE\n" +
    "                            dcp.prod_item_id\n" +
    "                        END AS prod_id,\n" +
    "                        gu_id,\n" +
    "                        sessn_id,\n" +
    "                        chanl_id,\n" +
    "\t\t\t            sessn_chanl_id,\n" +
    "                        pltfm_id,\n" +
    "                        nav_page_value,\n" +
    "                        sessn_pv,\n" +
    "                        internal_result_sum,\n" +
    "                        nav_tracker_id,\n" +
    "\t\t\t\t\t\tnav_button_position,\n" +
    "                        nav_next_tracker_id,\n" +
    "                        detl_tracker_id,\n" +
    "                        detl_button_position,\n" +
    "                        cart_tracker_id,\n" +
    "                        city_id,\n" +
    "                        CASE\n" +
    "                           WHEN t2.biz_unit IS NOT NULL\n" +
    "                                OR length(t2.biz_unit) <> 0 THEN\n" +
    "                            t2.biz_unit\n" +
    "                           WHEN t3.biz_unit IS NOT NULL\n" +
    "                                OR length(t3.biz_unit) <> 0 THEN\n" +
    "                            t3.biz_unit\n" +
    "                           ELSE\n" +
    "                            t4.biz_unit\n" +
    "                        END AS biz_unit,\n" +
    "                        ordr_code,\n" +
    "                        -------这里是中文\n" +
    "                        gu_sec_flg    \n" +
    "                        \n" +
    "        FROM   {$DB_DW}.dim_comb_prod dcp\n" +
    "        RIGHT  OUTER JOIN {$DB_DW}.fct_traffic_navpage_path_detl t1\n" +
    "        ON     (t1.cart_prod_id = dcp.prod_id AND dcp.cur_flag = 1)\n" +
    "        LEFT   OUTER JOIN (SELECT cc.pm_id,\n" +
    "                                 dm.biz_unit\n" +
    "                          FROM   {$DB_DW}.dim_yhd_pm cc\n" +
    "                          JOIN   {$DB_DW}.dim_mrchnt dm\n" +
    "                          ON     dm.mrchnt_id = cc.sale_mrchnt_id\n" +
    "                          AND    dm.cur_flag = 1\n" +
    "                          AND    cc.cur_flag = 1) t2\n" +
    "        ON     t1.detl_pm_id = t2.pm_id\n" +
    "        LEFT   OUTER JOIN (SELECT cc.pm_id,\n" +
    "                                 dm.biz_unit\n" +
    "                          FROM   {$DB_DW}.dim_yhd_pm cc\n" +
    "                          JOIN   {$DB_DW}.dim_mrchnt dm\n" +
    "                          ON     dm.mrchnt_id = cc.sale_mrchnt_id\n" +
    "                          AND    dm.cur_flag = 1\n" +
    "                          AND    cc.cur_flag = 1) t3\n" +
    "        ON     t1.cart_pm_id = t3.pm_id\n" +
    "        LEFT   OUTER JOIN (SELECT cc.prod_id,\n" +
    "                                 MAX(dm.biz_unit) AS biz_unit\n" +
    "                          FROM   {$DB_DW}.dim_yhd_pm cc\n" +
    "                          JOIN   {$DB_DW}.dim_mrchnt dm\n" +
    "                          ON     dm.mrchnt_id = cc.sale_mrchnt_id\n" +
    "                          AND    dm.cur_flag = 1\n" +
    "                          AND    cc.cur_flag = 1\n" +
    "                          GROUP  BY cc.prod_id) t4\n" +
    "        ON     t1.cart_prod_id = t4.prod_id\n" +
    "        WHERE  (t1.navgation_page_flag = 1 OR length(t1.detl_page_type_id) > 0)\n" +
    "        AND    t1.ds = '{$label}'\n" +
    "        AND    nav_page_categ_id = 1\n" +
    "        ) aa;\n" +
    " \n" +
    " \n" +
    "--set mapred.reduce.tasks=20;  \n" +
    "insert overwrite table {$DB_DW}.rpt_trfc_cms partition\n" +
    "  (ds = '{$label}')\n" +
    "  SELECT '{$label}',\n" +
    "       a.gu_id,\n" +
    "       a.sessn_id,\n" +
    "       gu_sec_flg,\n" +
    "       MAX(a.sessn_pv),\n" +
    "       a.chanl_id,\n" +
    "       a.sessn_chanl_id,\n" +
    "       a.pltfm_id,\n" +
    "       a.city_id,\n" +
    "       a.nav_page_value,      \n" +
    "       COUNT(DISTINCT CASE\n" +
    "                WHEN nav_tracker_id > 0 AND (length(nav_button_position) = 0 OR nav_button_position IS NULL or nav_button_position = 'null') THEN\n" +
    "                 nav_tracker_id\n" +
    "                ELSE\n" +
    "                 NULL\n" +
    "             END) AS cms_pv,\n" +
    "       COUNT(DISTINCT CASE\n" +
    "                WHEN nav_next_tracker_id > 0 THEN\n" +
    "                 nav_next_tracker_id\n" +
    "                ELSE\n" +
    "                 NULL\n" +
    "             END) AS cms_next_pv,\n" +
    "       COUNT(DISTINCT CASE\n" +
    "                WHEN detl_tracker_id > 0\n" +
    "                     AND (length(detl_button_position) = 0 OR detl_button_position IS NULL) THEN\n" +
    "                 detl_tracker_id\n" +
    "                ELSE\n" +
    "                 NULL\n" +
    "             END) AS detl_pv,\n" +
    "       COUNT(DISTINCT(CASE\n" +
    "                         WHEN detl_tracker_id > 0\n" +
    "                              AND (length(detl_button_position) = 0 OR detl_button_position IS NULL) THEN\n" +
    "                          detl_tracker_id\n" +
    "                         ELSE\n" +
    "                          NULL\n" +
    "                      END)) + COUNT(DISTINCT(CASE\n" +
    "                                                WHEN cart_tracker_id > 0\n" +
    "                                                     AND length(detl_tracker_id) = 0 THEN\n" +
    "                                                 cart_tracker_id\n" +
    "                                                ELSE\n" +
    "                                                 NULL\n" +
    "                                             END)) AS prdt_detl_pv,\n" +
    "       COUNT(DISTINCT CASE\n" +
    "                WHEN cart_tracker_id > 0 THEN\n" +
    "                 cart_tracker_id\n" +
    "                ELSE\n" +
    "                 NULL\n" +
    "             END) AS cart_pv,\n" +
    "       COUNT(DISTINCT(CASE\n" +
    "                         WHEN cart_tracker_id > 0\n" +
    "                              AND length(detl_tracker_id) = 0 THEN\n" +
    "                          cart_tracker_id\n" +
    "                         ELSE\n" +
    "                          NULL\n" +
    "                      END)) AS dirct_cart_pv,\n" +
    "       b.parnt_ordr_id,\n" +
    "       MAX(b.end_user_id),\n" +
    "       MAX(b.ordr_tranx_activ_flg),\n" +
    "       MAX(b.actl_sale_amt),\n" +
    "       MAX(b.sale_num),\n" +
    "       a.biz_unit,\n" +
    "       1,\n" +
    "       getdatetime()\n" +
    "FROM   {$DB_TMP}.tmp_cms_sessn a\n" +
    "LEFT   OUTER JOIN {$DB_TMP}.tmp_cms_ordr_tranx b\n" +
    "ON     (a.ordr_code = b.parnt_ordr_code AND a.prod_id = b.prod_id)\n" +
    "GROUP  BY a.gu_id,\n" +
    "          a.sessn_id,\n" +
    "          gu_sec_flg,\n" +
    "          a.chanl_id,\n" +
    "          a.sessn_chanl_id,\n" +
    "          a.pltfm_id,\n" +
    "          a.city_id,\n" +
    "          a.nav_page_value,\n" +
    "          b.parnt_ordr_id,\n" +
    "          a.biz_unit;";
}
