rm(list=ls())

#load the package
library(stringdist)
setwd("~/hotel_match_gd")


# 读取高德数据
library(RMySQL)
con<-dbConnect(RMySQL::MySQL(),host='10.10.81.5',port=3306,dbname='ods',username='biuser',password='biuser')
dbSendQuery(con, "SET character_set_client = gbk")
dbSendQuery(con, "SET character_set_connection = gbk")
dbSendQuery(con, "SET character_set_results = gbk")
dbSendQuery(con,'SET NAMES utf8')
sqlstr <- "
SELECT c.code*100000000 + a.id AS hotelid,a.source_id,a.NAME as hotel_name
,a.pcode as prov_code,a.pname,c.code AS city_code
,a.city_name,a.ad_code as dis_code,a.ad_name AS disname
FROM crawer.gd_hotel a
LEFT JOIN ots.t_district b ON a.ad_code=b.code 
LEFT JOIN ots.t_city c ON b.cityId = c.cityid;
"
res<-dbSendQuery(con,sqlstr)
gd_hotels <- dbFetch(res, n = -1) # fetch all results
dbDisconnect(con)


# 读取去哪儿和乐住酒店数据
library(RMySQL)
con<-dbConnect(RMySQL::MySQL(),host='10.10.81.5',port=3306,dbname='ods',username='biuser',password='biuser')
dbSendQuery(con, "SET character_set_client = gbk")
dbSendQuery(con, "SET character_set_connection = gbk")
dbSendQuery(con, "SET character_set_results = gbk")
dbSendQuery(con,'SET NAMES utf8')
# sqlstr <- "
# SELECT a.id,a.hotel_name,a.prov_code,SUBSTR(prov.proname FROM 3) as Proname
# ,a.city_code,SUBSTR(city.cityname FROM 3) as cityname,a.dis_code,SUBSTR(dist.DisName FROM 3) as disname FROM ht.q_hotel a
# LEFT JOIN ots.t_city city ON a.city_code = city.`Code`
# LEFT JOIN ots.t_province prov ON a.prov_code = prov.`Code`
# LEFT JOIN ots.t_district dist ON a.dis_code = dist.`Code`
# "
sqlstr <- "SELECT id as hotelid,hotel_name,prov_code,city_code,dis_code FROM ht.q_hotel"
res<-dbSendQuery(con,sqlstr)
qn_lz_hotels <- dbFetch(res, n = -1) # fetch all results
dbDisconnect(con)


#读取省市区数据
library(RMySQL)
con<-dbConnect(RMySQL::MySQL(),host='10.10.81.5',port=3306,dbname='ots',username='biuser',password='biuser')
dbSendQuery(con, "SET character_set_client = gbk")
dbSendQuery(con, "SET character_set_connection = gbk")
dbSendQuery(con, "SET character_set_results = gbk")
dbSendQuery(con,'SET NAMES utf8')
sqlstr <- "SELECT SUBSTR(prov.proname FROM 3) AS proname,prov.code AS prov_code,SUBSTR(city.cityname FROM 3) as cityname,city.code AS city_code,SUBSTR(dt.DisName FROM 3) AS disname,dt.code AS dis_code FROM ots.t_district dt 
LEFT JOIN ots.t_city city ON dt.CityID = city.cityid
LEFT JOIN ots.t_province prov ON city.ProID = prov.ProID;"
res<-dbSendQuery(con,sqlstr)
prov_city_dis <- dbFetch(res, n = -1) # fetch all results
dbDisconnect(con)




# Part I 跑实时全量数据------------------------------------------------------------------

# 删除原数据
library(RMySQL)
con<-dbConnect(RMySQL::MySQL(),host='10.10.81.5',port=3306,dbname='ht',username='biuser',password='biuser')
dbSendQuery(con, "SET character_set_client = gbk")
dbSendQuery(con, "SET character_set_connection = gbk")
dbSendQuery(con, "SET character_set_results = gbk")
dbSendQuery(con,'SET NAMES utf8')
dbSendQuery(con, paste("TRUNCATE TABLE ots_ex_hotel_mapping"))
dbDisconnect(con)


# 数据预处理（需要去除掉主要重复词）
words <- c("酒店","宾馆","公寓","旅馆","商务","连锁","旅行社","主题","精品","客栈","招待所","快捷","酒楼","山庄","度假村","会所","中心","大厦",
           "旅社","旅店","旅舍","住宿部","大坪店","农家乐","生态园","假日","浴苑","二部","浴池","三部","一部","时尚","政府","\\(","\\)","（","）",
           "国际","中央大街店","通天街","日租")

# 定义酒店匹配函数：
# 1.去除酒店名中关键词；
# 2.在对应区域中查找酒店；
# 3.若2步中没有查到，在去哪儿区域为空的酒店中查找。 
# hotelname <- '乌鲁木齐众汇旅馆'

hotel_match <- function(hotelname){
  
  # 备份酒店名
  hotelsub <- hotelname
  
  #去除酒店名中地址信息
  Pro_code <- qn_lz_hotels_part[qn_lz_hotels_part$hotel_name == hotelname,'prov_code']
  Pro <- unique(prov_city_dis[prov_city_dis$prov_code==Pro_code,'proname'])
  Pro1 <- gsub("省|市|(壮族)|(自治区)|(维吾尔)|(回族)","",Pro)
  
  
  city_code <- qn_lz_hotels_part[qn_lz_hotels_part$hotel_name == hotelname,'city_code']
  city <- unique(prov_city_dis[prov_city_dis$city_code==city_code,'cityname'])
  city1 <- gsub("(市)|(自治州)|(瑶族)|(地区)|(布依族)|(苗族)|(侗族)|(彝族)|(土家族)|(哈萨克)","",city)
  
  dist_code <- qn_lz_hotels_part[qn_lz_hotels_part$hotel_name == hotelname,'dis_code']
  district <- prov_city_dis[prov_city_dis$dis_code==dist_code,'disname']
  district1 <- gsub("(瑶族)|(毛南族)|(回族)|(苗族)|(土家族)|(布依族)|(蒙古族)|(仫佬族)|(仡佬族)|(彝族)","",district)
  district2 <- gsub("区|县|市|镇|(街道)|(自治)","",district1)  
  
  city_rm <- unique(c(Pro,Pro1,city,city1,district,district1,district2))
  for(x in city_rm){
    hotelsub <- gsub(x,"",hotelsub)
  }
  # 酒店名中大写数字转化阿拉伯数字
  hotelsub <- gsub("壹","1",hotelsub)
  hotelsub <- gsub("贰","2",hotelsub)
  hotelsub <- gsub("叁","3",hotelsub)
  hotelsub <- gsub("肆","4",hotelsub)
  hotelsub <- gsub("伍","5",hotelsub)
  hotelsub <- gsub("陆","6",hotelsub)
  hotelsub <- gsub("柒","7",hotelsub)
  hotelsub <- gsub("捌","8",hotelsub)
  hotelsub <- gsub("玖","9",hotelsub)
  
  
  # 获取高德地图同区域酒店，作为匹配酒店
  gd_hotels_part <- subset(gd_hotels,dis_code==dist_code)
  # 如果该区域没有酒店，则在区域为空的酒店查找
  if(nrow(gd_hotels_part)==0)  return (c(hotelid=NA,hotel_name=NA,sim=0))
  
  
  # 计算酒店名之间相似度，并取相似度最大值作为匹配成功酒店
  # 先全名匹配，后缩略词匹配
  sim <- unlist(lapply(gd_hotels_part$hotel_name, function(x) 
    stringsim(hotelname,x,method='qgram',q=2))) 
  indx <- which.max(sim)
  m_sim <- max(sim)
  
  # 去除城市名匹配
  if(m_sim < 0.6)  {
    sim <- unlist(lapply(gd_hotels_part$hotel_name, function(x) 
      stringsim(hotelsub,x,method='qgram',q=2))) 
    indx <- which.max(sim)
    m_sim <- max(sim)
  }
  
  
  # 去除酒店名中关键词
  for(i in words){
    hotelsub <- gsub(i,"",hotelsub)
  }
  # 缩略词匹配
  if(m_sim < 0.6)  {
    sim <- unlist(lapply(gd_hotels_part$hotel_name, function(x) 
      stringsim(hotelsub,x,method='qgram',q=2))) 
    indx <- which.max(sim)
    m_sim <- max(sim)
  }
  
  #   # 对没有匹配上的酒店（相似度为0）在区域为空的酒店再查找
  #   if(m_sim == 0){
  #     qunaer_hotel_part <- qunaer_hotels
  #     sim <- unlist(lapply(qunaer_hotel_part$hotel_name, function(x) 
  #       stringsim(hotelsub,x,method='qgram',q=2))) 
  #     indx <- which.max(sim)
  #     m_sim <- max(sim)
  #   }
  
  # 返回匹配结果
  return (c(hotelid=gd_hotels_part[indx,'hotelid'],hotel_source_id=gd_hotels_part[indx,'source_id']
            ,hotel_name=gd_hotels_part[indx,'hotel_name'],sim=m_sim))
}

hotels_all <- data.frame()
dist_code <- unique(qn_lz_hotels$dis_code)
# i=497
# i=498
n_dis <- length(dist_code)
for(i in c(242:n_dis) ){
  
  #提取去哪儿-乐住city酒店数据
  qn_lz_hotels_part <- subset(qn_lz_hotels,dis_code == dist_code[i])
  # library(sqldf)
  # str_sql <- paste("SELECT * FROM qn_lz_hotels WHERE city_code='",
  #                  cities_code[i],"'",sep = "")
  # 
  # qn_lz_hotels_part <- sqldf(str_sql)  
  #提取高德地图city酒店数据
  gd_hotels_part <- subset(gd_hotels,dis_code == dist_code[i])
  # 没有数据跳过，说明目前该城市没有去哪儿酒店
  if(nrow(gd_hotels_part) == 0) next
  
  
  # 去哪儿酒店名中大写数字转化阿拉伯数字
  gd_hotels_part$hotel_name <- gsub("壹","1",gd_hotels_part$hotel_name)
  gd_hotels_part$hotel_name <- gsub("贰","2",gd_hotels_part$hotel_name)
  gd_hotels_part$hotel_name <- gsub("叁","3",gd_hotels_part$hotel_name)
  gd_hotels_part$hotel_name <- gsub("肆","4",gd_hotels_part$hotel_name)
  gd_hotels_part$hotel_name <- gsub("伍","5",gd_hotels_part$hotel_name)
  gd_hotels_part$hotel_name <- gsub("陆","6",gd_hotels_part$hotel_name)
  gd_hotels_part$hotel_name <- gsub("柒","7",gd_hotels_part$hotel_name)
  gd_hotels_part$hotel_name <- gsub("捌","8",gd_hotels_part$hotel_name)
  gd_hotels_part$hotel_name <- gsub("玖","9",gd_hotels_part$hotel_name)
  
  # 并行计算
  library(parallel)
  
  # 判断几核 detectCores()
  # 5核CPU计算匹配结果
  mc <- getOption("mc.cores", 6)
  res <- mclapply(qn_lz_hotels_part$hotel_name,hotel_match, mc.cores = mc)
  
  # 匹配结果处理
  n <- length(qn_lz_hotels_part$hotel_name)
  qn_lz_hotels_part$gd_hotelid <- unlist(res)[(1:n)*4-3]
  qn_lz_hotels_part$gd_hotel_source_id <- unlist(res)[(1:n)*4-2]
  qn_lz_hotels_part$gd_hotel_name <- unlist(res)[(1:n)*4-1]
  qn_lz_hotels_part$sim <- unlist(res)[(1:n)*4]
  
  # 对没有匹配上的酒店，匹配结果改为NA
  if(nrow(qn_lz_hotels_part[qn_lz_hotels_part$sim==0,])!=0){
    qn_lz_hotels_part[qn_lz_hotels_part$sim==0,]$gd_hotelid <- NA
    qn_lz_hotels_part[qn_lz_hotels_part$sim==0,]$gd_hotel_source_id <- NA
    qn_lz_hotels_part[qn_lz_hotels_part$sim==0,]$gd_hotel_name <- NA
  }
  
  # 获取输出酒店
  library(sqldf)
  qn_lz_hotels_part <- sqldf('select hotelid, hotel_name, gd_hotelid,gd_hotel_source_id,gd_hotel_name,max(sim)
                           from qn_lz_hotels_part where gd_hotelid is not null group by hotel_name')
  if(nrow(qn_lz_hotels_part) == 0) next
  
  hotels <- qn_lz_hotels_part[,c('hotelid','hotel_name','gd_hotelid','gd_hotel_source_id','gd_hotel_name')]
  names(hotels) <- c("ots_hotel_id","ots_hotel_name","ex_hotel_id","ex_hotel_source_id","ex_hotel_name")
  hotels$site <- 'gd'
  
  hotels_all <- rbind(hotels_all,hotels)
  cat(i,'\n')
}


# 写入数据库 ods.ots_ex_hotel_mapping_test（全量数据）
library(RMySQL)
con<-dbConnect(RMySQL::MySQL(),host='10.10.81.5',port=3306,dbname='ht',username='biuser',password='biuser')
dbSendQuery(con, "SET character_set_client = gbk")
dbSendQuery(con, "SET character_set_connection = gbk")
dbSendQuery(con, "SET character_set_results = gbk")
dbSendQuery(con,'SET NAMES utf8')

dbWriteTable(con,"ots_ex_hotel_mapping",hotels_all, append = T, row.names = F)
dbDisconnect(con)
