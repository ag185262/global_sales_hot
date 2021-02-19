package com.ncr.edl.apollo.global_sales_hot

import com.ncr.edl.apollo.common.ExecutorDao
import java.sql.Timestamp
import java.util.Date

class globalsaleshotgems(executor: ExecutorDao, dbParams: scala.collection.mutable.Map[String, String]) {

  def run(){
    var query = ""

    query = """ INSERT
	                  OVERWRITE
					              TABLE """ + dbParams("EEDDLLDDBB") + """.global_sales_hot_gems_neos_customer_incident_data
                            SELECT
	                              SQ.master_number,
	                              SQ.product_id,
	                              SQ.product_id_fmt,
	                              SQ.country_cd,
	                              SQ.Region,
	                              SQ.product_name,
	                              SQ.master_name,
                                count(SQ.inc_id) as incident_count
           						      FROM
                                (
                                SELECT
                                    nsm_incident.incident_id as inc_id,
                                    master_number,
                                    master_name,
                                    product_id,
                                    product_id_fmt,
                                    product_description,
                                    nsm_incident.status,
                                    provider_instance,
                                    defect_escalation_flag,
                                    nsm_incident.country_code as country_cd,
                                    case
                                        when provider_instance='9036' then nsm_incident.productversion
                                        when provider_instance='8036' then supp.product_categorization_tier_3
                                        else supp.product_categorization_tier_3
                                    end as product_name,
                                    CASE
                                        WHEN provider_instance = '8036' THEN solution_end
                                        WHEN provider_instance = '9036' THEN incident_end
                                    END AS incident_resolution_date_time,
                                    CASE
                                        WHEN team = 'HSR' AND team_type = 'Aloha-L2' AND team_group = 'Verifone' THEN 'Hosp SW Sup'
                                        WHEN team = 'HSR' AND team_type = 'Aloha-L3' AND team_group = 'Verifone' THEN 'Hosp SW Sup'
                                        WHEN (team = 'HSR' AND team_type = 'QSR' AND team_group IN ('QSR Support','Security','WIT') AND priority <> '4') THEN 'Hosp SW Sup'
                                        WHEN team = 'HSR' AND team_type = 'PCR-PRAGUE' AND priority <> '4' THEN 'Ret SW Sup'
                                        WHEN (team = 'Retail Software Support' AND team_type IN ('ACS-IR','Advanced Store','AMS','Retail Counterpoint', 'GEMS','Connected Services','Retail Hosted Applications','FDMM L3 Support','ARS','Next Gen FDMM L3 Support')) THEN 'Ret SW Sup'
                                        WHEN (team = 'Retail Software Support' AND team_type = 'SI Support' AND problem_type <> 'Connected Payments') THEN 'Ret SW Sup'
                                        WHEN (team = 'Retail Software Support' AND team_type = 'ACS' AND (Fixed_user IS NULL OR fixed_user <> 'gemsneos') AND initiated_by <> 'Transfer') THEN 'Ret SW Sup'
                                        WHEN (team = 'Retail Software Support' AND team_type = 'Storepoint' AND master_number <> '9300726') THEN 'Ret SW Sup'
                                        WHEN team = 'Retail Software Support' AND team_type = 'Global Payments Support'  THEN 'Connected Payments'
                                        WHEN team = 'Retail Software Support' AND team_type = 'SI Support' AND problem_type = 'Connected Payments' THEN 'Connected Payments'
                                        WHEN team = 'Retail Software Support' AND team_type = 'IL Intl Support Team' AND team_group IN ('HQ\Loyalty','Storeline\LPE','Storepoint') THEN 'Ret SW Sup'
                                        WHEN team_type = 'SSCO' THEN 'Ret SW Sup'
                                        WHEN (team = 'Financial Software Support' AND (severity <> 'Proactive' OR severity IS NULL)  AND SUBSTR(team_type,1,2) <> 'PS' AND team_type <> 'Americas Interactive Ser_Desk') THEN 'Fin SW Sup'
                                        WHEN (team = 'ServiceDesk' AND team_type = 'Retalix' AND team_group = 'Dealer Channel Support') THEN 'Ret SW Sup'
                                        WHEN (team = 'HSR' AND team_type = 'PCR-AMERICAS' AND team_group IN ('Lead', 'Level II','Level III','Level IV','3rd Party') AND problem_code <> 'Request' AND nsm_incident.status_code NOT IN ('HW Sent','Dispatched') AND (fixed_user <> 'aradmin' OR fixed_user IS NULL)) THEN 'Ret SW Sup'
                                        WHEN (team = 'HSR' AND team_type IN ('HSR/Aloha ASPAC','HSR/Aloha INTL L2','HSR/Aloha INTL L3','HSR/Aloha Support L1','HSR/Aloha Support L2','HSR/Aloha Support WIT','HSR/Aloha Support L3','HSR Hosted')) THEN 'Hosp SW Sup'
                                        WHEN (team = 'HSR' AND team_type = 'Aloha-L1' AND team_group IN ('Corporate/Reseller','Local Office','Major Accounts','TGIF_CMT','Wingstop_CMT')) THEN 'Hosp SW Sup'
                                        WHEN (team = 'HSR' AND team_type = 'Aloha' AND team_group IN ('After Hours','Burger King NZ','HME','Call Management','Corporate','Direct CMT Escalations','Incident Management','MenuLink','Partner','Partner_WIT','Spanish Support','Spanish Support CLA','Spanish Support L2','UK Helpdesk','WIT','Wingstop_CMT','Wingstop_IMT','Wingstop_NBO'))  THEN 'Hosp SW Sup'
                                        WHEN (team = 'HSR' AND team_type = 'Aloha-L2' AND team_group IN ('Corporate/Reseller','Local Office','Major Accounts')) THEN 'Hosp SW Sup'
                                        WHEN (team = 'HSR' AND team_type = 'Aloha-L3' AND team_group IN ('Corporate/Reseller','Local Office','Major Accounts','Menulink','TGIF_IMT','TGIF_NBO','TGIF_WIT','WIT','WIT - Local Office','Wingstop_IMT','Wingstop_NBO')) THEN 'Hosp SW Sup'
                                        WHEN (team = 'HSR' AND team_type = 'Cinema' AND team_group IN ('Cinema-L1','Cinema-L2','Cinema-L3','WIT') AND problem_type <> 'Hardware') THEN 'Hosp SW Sup'
                                        WHEN (team = 'HSR' AND team_type = 'Hospitality_LON' AND team_group IN ('CMT Escalations','Hospitality_FRANCE','Hospitality_LON','Voicemail')) THEN 'Hosp SW Sup'
                                        WHEN (team = 'HSR' AND team_type = 'Hospitality_ASPAC' AND team_group IN ('L2','L3','Minor Helpdesk','WENDY''S INDIA')) THEN 'Hosp SW Sup'
                                        WHEN (team = 'HSR' AND team_type = 'Hospitality_PRAGUE' AND team_group IN ('Direct Customers','Reseller/Partner','WIT')) THEN 'Hosp SW Sup'
                                        WHEN (team = 'HSR' AND team_type = 'Hospitality_TDS' AND team_group IN ('Dunkin','Vitalcast','WIT') AND resolution_code NOT LIKE '%replace%')  AND problem_type <> 'Quote' THEN 'Hosp SW Sup'
                                        WHEN (team = 'HSR' AND team_type = 'Hosted' AND team_group IN ('Aloha Loyalty','Aloha Loyalty L3','Aloha Stored Value','Aloha Stored Value L3','Command Center','Command Center L3','Configuration Center','Configuration Center L3','Customer Voice','Hosted','Insight Polling','Insight Polling L3','Insight Reports','Insight Reports L3','Mobile Pay','Mobile Pay L3','NCR Console','Online Ordering','Online Ordering L3','Open Table','Pulse','Pulse L3','Restaurant Guard','WIT')) THEN 'Hosp SW Sup'
                                        WHEN (team = 'HSR' AND team_type = 'Local_Office' AND team_group IN ('Central CMT Escalations','Central_Support','Voicemail','WIT')) THEN 'Hosp SW Sup'
                                        WHEN (team = 'HSR' AND team_type = 'NSS' AND team_group = 'NSS' AND problem_code NOT IN ('Monitoring','Project','LD Learn','RMA')) THEN 'Hosp SW Sup'
                                        WHEN (team = 'HSR' AND team_type = 'Quest' AND team_group IN ('Quest-L1','Quest-L2','Quest-L3','WIT') AND problem_code <> 'Hardware'  AND nsm_incident.status_code <> 'Cancelled') THEN 'Hosp SW Sup'
                                        WHEN team = 'HSR' AND team_type = 'Aloha' AND team_group IN ('CP Support','Connected Payments','WIT') THEN 'Connected Payments'
                                        WHEN team = 'HSR' AND team_type IN ('PCR-ASIA L3','PCR-Petron L3') THEN 'Ret SW Sup'
                                        WHEN team = 'HSR' AND team_type = 'Aloha' AND team_group IN ('Hardware','Wingstop_HW','Spanish Support CLA Hardware')  THEN 'Hosp HW Sup'
                                        WHEN team = 'HSR' AND team_type = 'Aloha-L3' AND team_group IN ('Hardware','Wingstop_HW','TGIF_HW') THEN 'Hosp HW Sup'
                                        WHEN team = 'HSR' AND team_type IN ('Hospitality_LON','Hospitality_PRAGUE') AND team_group = 'Hardware' THEN 'Hosp HW Sup'
                                        WHEN team = 'HSR' AND team_type = 'Hospitality_TDS' AND team_group = 'Accuview' THEN 'Hosp HW Sup'
                                        WHEN team = 'HSR' AND team_type = 'Hospitality_TDS' AND resolution_code IN ('Battery Replaced locally','C.E. REPAIREDNo.of parts replaced:1','Hard Drive Replaced','Mas Ham Replaced','New UPS & Batteries replaced','New UPS & Batteries Replaced locally','New UPS & Battery Replaced','New UPS Replaced','New UPS Replaced locally','New UPS&Batteries Replaced','Parts Replaced','Repair/Replace LED Display','Replace Actineon Media Player','REPLACE CARDS','REPLACE DEF PART','REPLACE DEF PT +SEC','Replace Equipment, Other','Replace harddisk','Replace Hardware','Replace NCR Media Player','Replace NEC Display','Replace NEC Media Player','Replace Outdoor Samsung Display','Replace Parts','Replace Power Filter','Replace Samsung Display','Replace Seneca Media Player','Replace Switch','Replace Undefined Media Player','Replaced Hardware','REPLACED PART','Resolution Description: Replaced hard drive pm unit tested a','UPS Replaced','UPS& Batteries Replaced','Whole Unit Replacement') THEN 'Hosp HW Sup'
                                        WHEN team = 'HSR' AND team_type = 'Quest' AND problem_code = 'Hardware' THEN 'Hosp HW Sup'
                                        WHEN (team = 'Travel Software Support' AND team_type IN ('Travel Production','Netkey')) THEN 'Hosp SW Sup'
                                        WHEN team = 'TSO' AND team_type = 'GX002R14-TSO RET EMEA IT L2' THEN 'Ret SW Sup'
                                        WHEN team_type IN ('APTRA Clear','APTRA Ncompass','APTRA Passport','Authentic','ImageMark Archive','Transaction Gateway','Transaction Manager','TxInfinity','CxServer','NCR TH Server','Global Support CoE/Activate','Global Support CoE/US Edge','Global Support CoE/Advance NDC','Global Support CoE/Security','Global Support CoE/XFS','Americas Self Service','Americas Self Service','Americas Interactive Banking/Software Support','Europe Financial','Europe Financial','MEA Financial','Americas Interactive Banking/Upgrade Team','Americas Interactive Banking/Upgrade Team','Americas Interactive Banking/Hardware Support','Americas Interactive Banking/Operations Desk','Americas Interactive Banking/Hardware Support','Americas Interactive Banking/Operations Desk','Americas Interactive Banking/Software Support') THEN 'Fin SW Sup'
                                        WHEN team = 'HSR' AND team_type = 'Hosted' AND team_group IN ('Install','RGA Install') THEN 'Hosp Install Sup'
                                        --------------------------------NEOS-----------------------------------------------
                                        WHEN team = 'HSR/TDS Accuview L3' THEN 'Hosp HW Sup'
                                        WHEN team IN ('HSR/TDS Vitalcast L3','HSR/TDS Dunkin L3') AND resolution_code = 'Digital Signage' AND resolution_type IN ('Dunkin','Vitalcast') AND cause_code IN ('Replace','Repair') THEN 'Hosp HW Sup'
                                        WHEN team IN ('HSR/TDS Vitalcast L3','HSR/TDS Dunkin L3')  THEN 'Hosp SW Sup'
                                        WHEN team IN ('Commerce Retail / Payments Gateway L1','Commerce Retail / Payments Gateway L2','Commerce Retail / Payments Gateway L3','Commerce Retail / Payments Gateway L4') THEN 'Connected Payments'
                                        WHEN team IN ('HSR/Hosted Install','HSR/Hosted RGA Install') THEN 'Hosp Install Sup'
                                        WHEN team IN ('HSR/Quest L3', 'HSR/Quest WIT', 'HSR/Aloha INTL WIT L3') THEN 'Hosp SW Sup'
                                        WHEN team IN ('HSR/Hosted','HSR/Hosted Aloha Stored Value','HSR/Hosted Command Center','HSR/Hosted Customer Voice','HSR/Hosted Insight Polling','HSR/Hosted Online Ordering','HSR/Hosted Pulse','HSR/Hosted Restaurant Guard','HSR/Hosted Aloha Loyalty L3','HSR/Hosted COM L3','HSR/Hosted Command Center L3','HSR/Hosted Configuration Center L3','HSR/Hosted Insight Polling L3','HSR/Hosted Insight Reports L3','HSR/Hosted Mobile Pay L3','HSR/Hosted NCR Console L3','HSR/Hosted Online Ordering L3','HSR/Hosted Pulse L3','HSR/Hosted Stored Value L3','HSR/NSS L3','HSR/Hosted BSP L3','HSR/Hosted WIT L3','HSR/Hosted WIT')  THEN 'Hosp SW Sup'
                                        WHEN team IN ('Commerce Retail / CSS Italy','Commerce Retail / ACS L3','Commerce Retail / ACS-IR L3','Commerce Retail / AMS L3','Commerce Retail / Power HQ L3','Commerce Retail / Advanced Store L3','Commerce Retail / SSCO L3 (Self-Serv Check Out)','Commerce Retail / Dealer Channel') THEN 'Ret SW Sup'
                                        WHEN team = 'HSR/Quest' THEN 'Hosp SW Sup'
                                        WHEN team IN ( 'HSR/Aloha Support WIT', 'HSR/Aloha Support L2', 'HSR/Aloha Support L3', 'HSR/Aloha Support L1', 'HSR/Aloha INTL L2', 'HSR/Aloha INTL L3', 'HSR/Aloha ASPAC', 'HSR/Quest', 'HSR/Aloha INTL WIT', 'HSR/Travel Netkey L3', 'HSR/Hosted L3' ) AND team_type IN ('Support') THEN 'Hosp SW Sup'
                                        WHEN team IN ('HSR/Aloha Support Hardware', 'HSR/Aloha INTL HW','HSR/Aloha INTL CLA HW L3','HSR/Aloha Support Hardware L3','HSR/Aloha Wingstop Hardware L3') AND team_type IN ('Support') THEN 'Hosp HW Sup'
                                        WHEN team IN ( 'Commerce Retail / ABO L3', 'Commerce Retail / SSCO L3 (Self-Serv Check Out)', 'Commerce Retail / GEMS L3 (Global Ent, Merch & Supply Chain)', 'Commerce Retail / CFR L3 (Convenience, Fuel, Retail)', 'Commerce Retail / ACS-IR L3', 'Commerce Retail / Advanced Store L3','Commerce Retail / Dealer Channel', 'Commerce Retail / Power HQ L3', 'Commerce Retail / Smart Retail L3', 'Commerce Retail / Emerald L4', 'Commerce Retail / APAC Support Team', 'Commerce Retail / Emerald L3' ) AND team_type IN ('Support') THEN 'Ret SW Sup'
                                        WHEN team IN ('BANKING / ATM CoE Activate','BANKING / ATM CoE Edge','BANKING / ATM CoE NDC','BANKING / ATM CoE Security','BANKING / ATM CoE XFS','BANKING / ATM Direct Americas CLA','BANKING / ATM Direct Americas NAMER','BANKING / ATM Direct EMEA ATMfutura','BANKING / ATM Direct EMEA Europe','BANKING / ATM Direct EMEA MEA','BANKING / ATM Direct Operations Desk','BANKING / CM Cash Management','BANKING / CM Systems Management','BANKING / PS NAMER TG Services','BANKING / TPBO Archive','BANKING / TPBO Channel Services Platform','BANKING / TPBO NCompass','BANKING / TPBO Passport','BANKING / TPBO Transaction Gateway','BANKING / TPBO Transaction Manager','BANKING / TPES APTRA Clear','BANKING / TPES Authentic','BANKING / TPES Connections Server','BANKING / COCES Connections Server', 'BANKING / COCES NCR TH Server','BANKING / ATM Direct Americas NAMER ATM','BANKING / ATM Direct Americas NAMER ITM')
                                        THEN 'Fin SW Sup'
                                        WHEN team IN ('Commerce Retail / Power + WMS L3','Commerce Retail / Power Analyzer L3','Commerce Retail / Power Biceps, ABS, Prompt L3','Commerce Retail / Power Care L3','Commerce Retail / Power DAX L3','Commerce Retail / Power Delivery L3','Commerce Retail / Power Dock L3','Commerce Retail / Power Enterprise L3','Commerce Retail / Power HQ L3','Commerce Retail / Power Integrator L3','Commerce Retail / Power Menu L3','Commerce Retail / Power Mobile L3','Commerce Retail / Power Net L3','Commerce Retail / Power Purchasing L3','Commerce Retail / Power Sell L3','Commerce Retail / Power Transportation L3','Commerce Retail / Power Triceps L3','Commerce Retail / Power Warehouse L3','Commerce Retail / Power Yard L3','Commerce Retail / R10 L3','Commerce Retail / ARS Germany','Commerce Retail / EMEA Support Team L3','Commerce Retail / APAC TSS support L2 ','Commerce Retail / StoreLine LPE APAC & EMEA L3','Commerce Retail / HQ Loyalty APAC & EMEA L3','Commerce Retail / CP ALL L2','Commerce Retail / StorePoint INTL IL L3','Commerce Retail / Connected Services','Commerce Retail / Storepoint','Commerce Retail / Storepoint NAMER L2','Commerce Retail / Storepoint NAMER L3','Commerce Retail / FDMM L3', 'Commerce Retail / Power MDM L3','Commerce Retail / Power Picking L3','Commerce Retail / CFR RPOS NAMER L2') THEN 'Ret SW Sup'
                                        WHEN team IN ('HSR/Hosted Cloud Connect L3','HSR / NSS L3','HSR/Aloha INTL ASPAC L3','HSR/Aloha INTL CLA L3','HSR/Aloha INTL EMEA L3','HSR/Aloha INTL HME L3','HSR/Aloha INTL Reseller Partner L3','HSR/Aloha INTL Spain L3','HSR/Aloha INTL WIT L3','HSR/Aloha Menulink NBO L3','HSR/Aloha NAMER Corporate Reseller L3','HSR/Aloha NAMER Local Office L3','HSR/Aloha NAMER Major Accounts L3','HSR/Aloha NAMER WIT L3','HSR/Aloha Popeyes L3','HSR/Aloha Wingstop L3','HSR/Aloha Support L1 - INTL','HSR/Aloha Support L1 - INTL FR','HSR/Aloha Support L1 - LO','HSR/Aloha Support L1 - MA','HSR/Aloha Support L1 - SD','HSR/Aloha Support L2 - INTL','HSR/Aloha Support L2 - INTL FR','HSR/Aloha Support L2 - LO','HSR/Aloha Support L2 - MA','HSR/Aloha Support L2 - SD','HSR/Aloha Support L2 - VF','HSR/Aloha Wingstop NBO L3','HSR/Aloha Wingstop L1','HSR/Aloha Wingstop L2','HSR/Hosted L3','HSR/QSR L3','HSR/Hosted L1 L2','HSR/Aloha Support L2 - SD','HSR/QSR WIT') THEN 'Hosp SW Sup'
                                        ELSE 'Other'
									                  END AS support_type_sw,
                                    CASE
                                        WHEN team =  'help_desk' AND master_name =  'FORCEPOINT' AND initiated_by <> 'Predictive' THEN 'T & T Team'
                                        WHEN team =  'help_desk' AND initiated_by <>  'Predictive' AND ( master_name  LIKE  '7728%'  OR  master_name LIKE  'AUDIO CODES%'  OR master_name LIKE  'FRONTIER%'
                                        OR  master_name LIKE  'FIFTH THIRD BANK OF CINCINNATI%'   OR master_name  LIKE  'MCAFEE%'  OR master_name LIKE  'MGM MIRAGE%') THEN'T & T Team'
                                        WHEN provider_instance = '8036' AND initiated_by <>  'Predictive' AND  (master_name IN  ( 'Great Clips','Fresh Market','The Fresh Market','METROLINX','GREYHOUND','Ultra Electronics','Hyatt','Hyatt Corporation','Hyatt Place')
                                        OR master_name LIKE  '%Checkers%' OR master_name LIKE  '%Craftworks%')  THEN 'Checkers'
                                        WHEN  initiated_by <>  'Predictive' AND (master_name = 'Sainsbury Supermarket' OR master_name = 'Sainsburys') THEN 'Sainsburys'
                                        WHEN team =  'NSMC' AND master_name =  'Accenture' AND market_name =  'Sainsbury' AND initiated_by <>  'Predictive'  THEN 'Sainsburys'
                                        WHEN team IN  ( 'Service Desk','SH L2 Service Desk') AND master_name IN  ( 'BEIJING BANGYUEHAN CATERING DEVELOPMENT CO., LTD.','BEIJING ESQUIRES MANAGEMENT CO., LTD','BLUE HORIZON HOSPITALITY GROUP','BURGER KING CHINA','FAST GOURMET GROUP(DUNKIN DONUTS)','GOLDEN CUP CHINA','GUNG HO PIZZA CHINA','HUALIAN COSTA (BEIJING) FOOD& BEVERAGE CO., MANAGE','MAXIM''S CATERERS LIMITED','MIX DELICACY','SHELL (CHINA) LIMITED'  ) AND provider_instance = '8036' AND initiated_by <>  'Predictive' THEN  'CHINA HSR'
                                        WHEN team = 'HSR' AND team_type = 'Hospitality_ASPAC' AND master_name IN   ( 'BEIJING BANGYUEHAN CATERING DEVELOPMENT CO., LTD.','BEIJING ESQUIRES MANAGEMENT CO., LTD','BLUE HORIZON HOSPITALITY GROUP','BURGER KING CHINA','FAST GOURMET GROUP(DUNKIN DONUTS)','GOLDEN CUP CHINA','GUNG HO PIZZA CHINA','HUALIAN COSTA (BEIJING) FOOD& BEVERAGE CO., MANAGE','MAXIM''S CATERERS LIMITED','MIX DELICACY') AND provider_instance = '9036' AND initiated_by <>  'Predictive' THEN  'CHINA HSR'
                                        WHEN team IN ( 'HSR','Service Desk','SH L2 Service Desk') AND master_name = 'BURGER KING TAIWAN-HOME CHAIN FOODS CO., LTD.'  THEN 'CHINA HSR'
                                        WHEN master_name =  'Australia Post' AND provider_instance = '8036' AND initiated_by <>  'Predictive' THEN  'Australia HD'
                                        WHEN team IN ( 'help_desk','Service Desk') AND master_name IN  ( 'COLES SUPERMARKET','KMART','TARGET AUSTRALIA P/L') THEN  'CEBU HD'
                                        WHEN master_name =  'HUNGRY JACK''S PTY LTD' AND provider_instance = '8036' AND initiated_by <>  'Predictive' THEN  'Australia HD'
                                        WHEN team =  'HSR'
										                    THEN
                                            CASE
									                              WHEN team_type IN  ( 'Petron L1','Strateq Malaysia','PCR-Petron L3','Hospitality_ASPAC'  ) AND master_name IN  ( 'PETRON MALAYSIA REFINING & MARKETING BHD','PETRON OIL (M) SDN BHD','PETRON FUEL INTERNATIONAL SDN BHD','PETRONOIL (M) SDN BHD'  ) THEN  'Malaysia HD'
                                                WHEN team_type IN  ( 'PCR-ASPAC','PCR-ASPAC L1','PCR-ASPAC L2'  ) AND problem_code =  'Indep - Incident' THEN  'CEBU HD'
                                            END
                                        WHEN provider_instance = '9036'  and team =  'help_desk' AND team_type =  'SSCO' AND master_name =  'NTUC FAIRPRICE CO-OPERATIVE LTD' THEN  'Malaysia HD'
                                        WHEN provider_instance = '8036'  and team  NOT IN  ( 'Service Desk','L2 Managed Services / Customer Connectivity'  ) AND master_name =  ' NTUC FairPrice' THEN  'Malaysia HD'
                                        WHEN team IN ( 'help_desk','C4 ARG Gestion De Usuarios','C4 ARG Operador Tecnico','Carrefour AR - Gestion de Usuarios'  ) AND master_name IN  ( 'Carrefour','Carrefour Inc S A'  ) AND initiated_by <>  'Predictive' THEN 'CARREFOUR Team'
                                        WHEN team IN  ( 'help_desk','Service Desk'  ) AND master_name NOT IN  ( 'Carrefour','Carrefour Inc S A','Sabre','Arcos Dorados C&LA') AND team_location =  'Argentina' AND initiated_by <>  'Predictive' THEN 'PHILIPS/GIBSON Team'
                                        WHEN master_name =  'Argos Limited' AND initiated_by <>  'Predictive' THEN  'Argos'
                                        WHEN team IN  ( 'help_desk','Service Desk','BKUK Field Support','HSR/Aloha INTL HW','HSR/Aloha INTL L2','HSR/Aloha INTL L3','HSR/Aloha Support L1'  ) AND master_name =  'Burger King UK' THEN  'CANADA HELP DESK'
                                        WHEN team IN  ( 'help_desk','Service Desk') AND UPPER(master_name) = 'BLOOMIN BRANDS INC' THEN  'CANADA HELP DESK'
                                        WHEN team IN  ( 'help_desk','Service Desk'  ) AND initiated_by <>  'Predictive' AND (master_name LIKE  'British%' OR master_name LIKE  'Post Office LTD%') THEN  'MARKS & SPENCER Team'
                                        WHEN master_number IN  ( '10263860','10265359','10265016','10275558','10265356','10275559','10384054','10394907') AND initiated_by <> 'Predictive' THEN 'Inditex +'
                                        WHEN master_number IN  ('7524837','10265294') AND initiated_by <>  'Predictive' THEN  'AHOLD Team'
                                        WHEN master_number IN  ('7854572') AND team  IN  ( 'help_desk','Service Desk') THEN  'AHOLD Team'
                                        WHEN master_name LIKE  'Tesco Stores%' AND provider_instance = '8036' AND initiated_by <>  'Predictive' AND market_name LIKE '%75150%' THEN  'TESCO - UK'
                                        WHEN master_name LIKE  'Tesco Stores%' AND provider_instance = '8036' AND initiated_by <>  'Predictive' AND market_name NOT LIKE '%75150%'THEN  'TESCO - CE'
                                        WHEN team IN  ( 'help_desk','Service Desk'  ) AND master_name LIKE  'Tesco Stores%' AND provider_instance = '9036' AND initiated_by <>  'Predictive'AND market_name LIKE '%75150%' THEN  'TESCO - UK'
                                        WHEN team IN  ( 'help_desk','Service Desk'  ) AND master_name = 'WH SMITH RETAIL HOLDINGS LIMITED' AND initiated_by <>  'Predictive' THEN  'TESCO - UK'
                                        WHEN team IN  ( 'help_desk','Service Desk'  ) AND master_name LIKE  'Tesco Stores%' AND provider_instance = '9036' AND initiated_by <>  'Predictive' AND market_name NOT LIKE '%75150%' THEN  'TESCO - CE'
                                        WHEN provider_instance ='9036' AND team = 'help_desk'  AND master_name IN  ( 'Norma','Norma GMBH & CO.KG', 'BENNET SPA','Unicoop Firenze','Kaufland InformationsSysteme GMBH & CO.KG','KAUFLAND INFORMATIONSSYSTEME GMBH &','Billa S.R.O.') THEN  'Inditex +'
                                        WHEN provider_instance = '9036' AND team IN  ( 'help_desk','Retail Software Support') AND master_name  IN  ( 'REWE-INFORMATIONS-SYSTEME GMBH') THEN  'Inditex +'
                                        WHEN team IN  ( 'help_desk')  AND initiated_by <>  'Predictive' AND (master_name ='DANSK' OR master_name = 'Salling Group')  THEN  'TESCO - CE'
                                        WHEN provider_instance = '9036' AND team = 'help_desk'  AND master_name IN  ('ICELAND','POUNDLAND') THEN  'MARKS & SPENCER Team'
                                        WHEN team IN  ( 'help_desk','Service Desk'  ) AND initiated_by <>  'Predictive' AND master_name LIKE  'Marks & Spencer%'  THEN  'MARKS & SPENCER Team'
                                        WHEN provider_instance  =  '8036' AND master_name  IN  ( 'BENNET SPA','Unicoop Firenze','REWE-INFORMATIONS-SYSTEME GMBH')  AND team  IN  ( 'Service Desk') THEN  'Inditex +'
                                        WHEN provider_instance  =  '8036' AND master_name  IN  ('POUNDLAND','ICELAND') AND team  IN  ( 'Service Desk') THEN  'MARKS & SPENCER Team'
                                        WHEN provider_instance  =  '8036' AND master_name  IN  ('SALLING GROUP') AND team  IN  ( 'Service Desk') THEN  'TESCO - CE'
                                        WHEN provider_instance  =  '8036' AND master_name  IN  ( 'Norma GMBH & CO.KG') AND team  IN  ( 'Service Desk','Commerce Retail / ARS Germany') THEN  'Inditex +'
                                        WHEN provider_instance  =  '8036' AND master_name  IN  ( 'Billa S.R.O.') AND  team  IN  ( 'help_desk','Commerce Retail / SSCO L3 (Self-Serv Check Out)') THEN  'Inditex +'
                                        WHEN provider_instance  =  '8036' AND master_name  IN  ( 'Kaufland InformationsSysteme GMBH & CO.KG') AND team  IN  ( 'Commerce Retail / SSCO L3 (Self-Serv Check Out)','KFL L1 HD','Service Desk') THEN  'Inditex +'
                                        WHEN team IN  ( 'help_desk','Service Desk','McDonalds Spain L3'  ) AND master_name IN  ( 'McDonalds_ES','McDonalds Spain'  ) AND initiated_by <>  'Predictive'
                                        THEN 'Spain HD'
                                        WHEN team IN  ( 'JP_Help_Desk','Department Store','General Merchandising Store','KIOSK','Special Project','Specialty Store'  ) AND team_type <>  'NBS' AND initiated_by <>  'Predictive'
                                        THEN  'JAPAN HD'
                                        WHEN team IN  ( 'help_desk','Service Desk'  ) AND master_name IN  ( 'ABU DHABI AIRPORT COMPANY'  ) AND initiated_by <>  'Predictive' THEN  'Checkers'
                                        WHEN team IN  ( 'help_desk','Service Desk','NCR Field Service','RBS Field Service','NCR Depot Service','CrossCom_SD','RHHD L2','Commerce Retail / SSCO L3 (Self-Serv Check Out)'  ) AND (master_name IN  ( 'Popeyes Louisiana Kitchen,Inc','Popeyes','Popeyes FZ','Popeyes Louisiana Kitchen (Brand)','Burger King BKC',  'RBI - Tim Hortons'  ) OR master_name LIKE 'Burger King Corpor%') AND initiated_by <>  'Predictive'
                                        THEN  'CANADA HELP DESK'
                                        WHEN team IN  ( 'help_desk','Service Desk','NCR Field Service','RBS Field Service','NCR Depot Service','CrossCom_SD','RHHD L2','Commerce Retail / SSCO L3 (Self-Serv Check Out)'  ) AND master_name IN  ( 'Overwaitea Foods','OVERWAITEA') AND initiated_by <>  'Predictive' THEN  'Home Depot/Fastlane'
                                        WHEN team IN  ( 'L2 Retail / ACS','L2 Retail / PCR','Service Desk'  ) AND master_name IN  ( 'Sobeys'  ) AND provider_instance = '8036' AND initiated_by <>  'Predictive'
                                        THEN  'CANADA HELP DESK'
                                        WHEN team IN  ( 'help_desk','Service Desk','NCR Field Service','RBS Field Service','NCR Depot Service','CrossCom_SD','RHHD L2','Commerce Retail / SSCO L3 (Self-Serv Check Out)') AND initiated_by <>  'Predictive' THEN
                                            CASE
                                                WHEN master_name LIKE '%Royal Farms%' OR master_name LIKE 'OTG'  THEN 'Home Depot/Fastlane'
                                                WHEN master_name =  'FOOD LION INC' THEN 'Home Depot/Fastlane'
                                                WHEN master_name LIKE  '%Home Depot%' THEN 'Home Depot/Fastlane'
                                                WHEN master_name LIKE  'Meijer%'  THEN 'Home Depot/Fastlane'
                                                WHEN master_name LIKE 'Rite Aid%' OR master_name LIKE 'Hy-Vee%' OR master_name LIKE 'King Kullen%' OR master_name LIKE 'Quick%' OR master_name LIKE 'Wakefern%'
                                                OR master_name LIKE 'WEIS%' OR master_name LIKE 'Thornton%' OR master_name LIKE 'Weis%' OR master_name LIKE 'Winco%' OR master_name LIKE 'SHEETZ%'
                                                OR master_name LIKE 'Whole Foods%' OR master_name LIKE 'Bashas%'  OR master_name= 'FIRST COAST ENERGY' OR master_name= 'Parkers' THEN 'Home Depot/Fastlane'
                                            END
                                        WHEN team IN  ( 'Commerce Retail / Emerald L2'  ) AND master_name IN  ( 'Meijer','Meijer Inc','NORTHGATE GONZALEZ INC'  ) AND initiated_by <>  'Predictive' THEN 'Emerald'
                                        WHEN team =  'Financial Software Support' AND team_type IN  ( 'Americas Interactive Ser_Desk'  ) AND initiated_by <>  'Predictive' THEN 'HIGH AVAILABILITY Team'
                                        WHEN team =  'GMS' AND initiated_by <>  'Predictive' AND master_name =  'RBS' AND incident_type NOT IN  ('Change') THEN 'Worldpay'
                                        WHEN team =  'GMS' AND initiated_by <>  'Predictive' AND master_number =  '77031' AND incident_type NOT IN  ('Change') THEN 'Worldpay'
                                        ------------------ Call One ---------------
                                        WHEN  master_name = 'AT&T GOVERNMENT SOLUTIONS' THEN 'Call One'
                                        WHEN  master_name= 'CISCO - OEM MAINTENANCE' THEN 'Call One'
                                        WHEN  master_name='AT&T_MOW' and nsm_incident.country_code ='PR' THEN 'Call One'
                                        WHEN  team= 'call_one' and SUBSTR(master_name,1,5) ='CISCO' AND nsm_incident.country_code NOT IN ('AU','CN','DO','HK','ID','IN','JP','MY','NZ','PH','SG','TH','TW') THEN 'Call One'
                                        WHEN  team= 'call_one' and  SUBSTR(master_name,1,5) <> 'CISCO' AND master_name <> 'SEARS HOLDINGS CORPORATION' THEN 'Call One'
                                        WHEN  team= 'help_desk' AND
                                        (SUBSTR(master_name,1,8)='Tsystems' OR
                                        SUBSTR(master_name,1,8)='VODAFONE' OR
                                        SUBSTR(master_name,1,8)='Vodafone' OR
                                        SUBSTR(master_name,1,3)='ATT' OR
                                        SUBSTR(master_name,1,4)='AT&T') THEN 'Call One'
                                        WHEN team= 'Financial Software Support' and team_type = 'Transaction Manager' and specialist = 'X' THEN 'Call One'
                                        WHEN  (master_name= 'Brightwell Payments' or master_name = 'Brightwell Payment') and (problem_code <> '1st LINE DISPATCH' OR problem_code <> '2nd LINE DISPATCH') THEN 'Brightwell Payments'
                                        WHEN  team = 'ATM' AND last_vendor__ IS NOT NULL THEN  'Call One'
                                        WHEN master_name = 'SEARS HOLDINGS CORPORATION' AND service_type IN ('1','1ALT','2','2ALT','01','02')  THEN 'Call One'
                                        WHEN master_name = 'SEARS HOLDINGS CORPORATION' AND  SUBSTR(resource_name,1,9) = 'Exception' THEN 'Call One'
                                        ELSE 'Other'
									                  END AS team_name_sd,
                                        ------------------------------REGION--------------------------------------------------------------------------
                                    CASE
									                      WHEN nsm_incident.country_code IN ('WS','VU','VN','UZ','UM','TW','TO','TM','TL','TH','SG','SB','PW','PS','PH','PG','PF','NZ','NU','NP','NC','MY','MV','MO','MN','MM','MH','LK','LA','KZ','KR','KP','KH','KG','JP','IN','IL','ID','HK','FJ','CN','CK','BT','BN','AZ','AU','AS','AM') THEN 'APAC'
                                        WHEN nsm_incident.country_code IN ('VE','VC','UY','TT','TC','SX','SV','SR','PY','PR','PE','PA','NI','MX','MQ','MP','MF','LC','KY','KN','JM','HT','HN','GY','GU','GT','GP','GL','GF','GD','EC','DO','DM','CW','CU','CR','CO','CL','BZ','BS','BR','BQ','BO','BM','BB','AW','AR','AI','AG') THEN 'CLA'
                                        WHEN nsm_incident.country_code IN ('VG','UA','TR','SK','SI','SE','RU','RS','RO','PT','PL','NO','NL','MK','ME','MD','MC','LV','LU','LT','LI','KV','JE','IT','IS','IM','IE','HU','HR','GI','GG','GE','GB','FR','FO','FI','ES','EE','DK','DE','CZ','CH','BY','BG','BE','BA','AT','AL','AD') THEN 'EUROPE'
                                        WHEN nsm_incident.country_code IN ('ZW','ZM','ZA','YE','XK','XJ','UG','TZ','TN','TG','TD','SN','SL','SD','SC','SA','RW','RE','QA','PK','OM','NG','NE','NA','MZ','MW','MU','MT','MR','ML','MG','MA','LR','LB','KW','KE','JO','IQ','GW','GR','GQ','GN','GM','GH','GA','ET','EG','DZ','DJ','CY','CM','CI','CG','CF','CD','BW','BJ','BI','BH','BF','BD','AO','AF','AE') THEN 'MEA'
                                        WHEN nsm_incident.country_code IN ('VI','US','CA') THEN 'NAMER'
                                    END AS Region
                                    FROM
									                      """ + dbParams("TRANSDDBB") + """.nsm3_incidents nsm_incident
                                    LEFT JOIN
                                        (
                                            SELECT
											                          DISTINCT incident_id,
												                        'defect' AS defect_escalation_flag
                                            FROM """ + dbParams("TRANSDDBB") + """.nsm3_actions action
                                            WHERE provider_instance = '9036'
                                            AND (action_code IN ('Defect Escalation','nsws - Defect Escalation','Defect Withdrawn')
                                            OR action_description_code = 'Known JIRA Association')
										                    ) defect
                                    ON   nsm_incident.incident_id = defect.incident_id
                                    LEFT JOIN
                                       (
                                        SELECT
                                            incident_id ,
                                            product_categorization_tier_3,
                                            product_categorization_tier_2,
                                            product_categorization_tier_1
                                        FROM
                                            hub_mrf.gems_incident_supp
                                        ) supp
                                        ON nsm_incident.incident_id = supp.incident_id
                                    ) SQ
                                    WHERE(SQ.status <> 'Fixed'
                                    OR
									                  CAST(SQ.incident_resolution_date_time AS DATE) >= '2020-09-01'
									                  and cast (SQ.incident_resolution_date_time AS DATE) <= '2020-09-30')
									                  AND SQ.support_type_sw in('Hosp HW Sup','Hosp SW Sup','Ret SW Sup','Fin SW Sup','Connected Payments')
									                  and (SQ.master_number is not null or SQ.master_number <> '' or length(SQ.master_number) > 0)
                                    and SQ.Region='NAMER'
                                    GROUP BY
                                    SQ.master_number,
                                    SQ.product_id,
                                    SQ.product_id_fmt,
                                    SQ.country_cd,
                                    SQ.Region,
                                    SQ.product_name,
                                    SQ.master_name """
    executor.executeQuery(query)
  }
}

object globalsaleshotgemsObjRunner{

  def main(args: Array[String]){
    if (args.length < 1) {
      println("Property file path not provided.")
      sys.exit(1)
    }
    var propertyFilePath = args(0)
    var activityCountRequired = false
    //if(args.length > 1){
    //    activityCountRequired = args(1).toBoolean
    //}

    var executor = new ExecutorDao(activityCountRequired, "global_sales_hot_gems")
    var dbParams = executor.loadProperties(propertyFilePath)
    var job_start_time = new Timestamp(new Date().getTime())
    println("################################################################")
    println("Started Job 'globalsaleshotgems' at %s.".format(job_start_time.toString()))
    var globalsaleshotgemsObj = new globalsaleshotgems(executor, dbParams)
    try {
      globalsaleshotgemsObj.run()
    } finally {
      var job_end_time = new Timestamp(new Date().getTime())
      println("Completed 'globalsaleshotgems' at %s.".format(job_end_time.toString()))
      println("Total Job Execution Time: %s".format(executor.getExecutionTime(job_start_time, job_end_time)))
      executor.postProcess()
      println("################################################################")
    }
  }
}
