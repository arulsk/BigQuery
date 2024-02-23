const express = require('express');
const route = express.Router()
const bigqueryControl = require('../controller/bigquery')


route.get('/getBigQueryData',bigqueryControl.getdata)
route.get('/campaign_ID/:campaign_ID',bigqueryControl.campaign_ID)
route.get('/Ad_Group_ID/:Ad_Group_ID',bigqueryControl.Ad_Group_ID)
route.get('/FSN_ID/:FSN_ID',bigqueryControl.FSN_ID)
route.get('/product_Name/:product_Name',bigqueryControl.product_Name)
route.get('/campaign_Name/:campaign_Name',bigqueryControl.campaign_Name)
route.get('/maxViews',bigqueryControl.maxViews)
route.get('/minViews',bigqueryControl.minViews)
route.get('/maxClicks',bigqueryControl.maxClicks)
route.get('/minClicks',bigqueryControl.minClicks)
route.get('/maxAdSpend',bigqueryControl.maxAd_spend)
route.get('/minAdSpend',bigqueryControl.minAd_spend)
route.get('/totalOfEachCampaign_Id',bigqueryControl.uniqueCampaign)





module.exports = route
