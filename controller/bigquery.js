const {BigQuery } = require('@google-cloud/bigquery')
require('dotenv').config()
const bigquery = new BigQuery

const getdata = async(req,res)=>{

    try{
    const query = 'SELECT * FROM `sacred-catfish-415008.aruldb_dataset.fileData` '
    const [getData] = await bigquery.query(query)
    if(getData.length === 0){
        return res.status(404).json({error : "No Data  FOUND"});
       }
    res.status(200).json({getData,dataln : getData.length})
   }catch(err){
    console.log(err.message);
    res.status(500).json({ error : err.message });
   }
} 

const campaign_ID = async(req,res)=>{
    const campaign_ID = req.params.campaign_ID
    try{
        const query = `SELECT * FROM \`sacred-catfish-415008.aruldb_dataset.fileData\` WHERE Campaign_ID = '${campaign_ID}' `;
       const [getData] = await bigquery.query(query)
       if(getData.length === 0){
        return res.status(404).json({error : "No campaign_ID  FOUND"});
       }

     res.status(200).json(getData)
    }catch(err){
     console.log(err.message);
      res.status(500).json({ error : err.message });
    }
 }
 
 const FSN_ID = async(req,res)=>{
    const FSN_ID  = req.params.FSN_ID
    try{
        const query = `SELECT * FROM \`sacred-catfish-415008.aruldb_dataset.fileData\` WHERE FSN_ID = '${FSN_ID}' `;
       const [getData] = await bigquery.query(query)
       if(getData.length === 0){
        return res.status(404).json({error : "No FSN_ID FOUND"});
       }
     res.status(200).json(getData)
    }catch(err){
     console.log(err.message);
     res.status(500).json({error : err.message});
    }
 }

 const Ad_Group_ID = async(req,res)=>{
    const Ad_Group_ID  = req.params.Ad_Group_ID
    try{
       const query = `SELECT * FROM \`sacred-catfish-415008.aruldb_dataset.fileData\` WHERE Ad_Group_ID = '${Ad_Group_ID}'`;
       const [getData] = await bigquery.query(query)
       if(getData.length === 0){
        return res.status(404).json({error : "No Ad_Group_ID FOUND"});
       }
       res.status(200).json(getData)
    }catch(err){
     console.log(err.message);
     res.status(500).json({error : err.message});
    }
 } 

 const product_Name = async(req,res)=>{
    const product_Name  = req.params.product_Name
    try{
       const query = `SELECT * FROM \`sacred-catfish-415008.aruldb_dataset.fileData\` WHERE product_Name = '${product_Name}' `;
       const [getData] = await bigquery.query(query)
       if(getData.length === 0){
       return res.status(404).json({error : "No product_Name FOUND "});
       }
       res.status(200).json(getData)
    }catch(err){
     console.log(err.message);
     res.status(500).json({error : err.message});
    }
 } 

 const campaign_Name = async(req,res)=>{
    const campaign_Name   = req.params.campaign_Name
    try{
       const query = `SELECT * FROM \`sacred-catfish-415008.aruldb_dataset.fileData\` WHERE campaign_Name  = '${campaign_Name}'`;
       const [getData] = await bigquery.query(query)
       if(!getData || getData.length === 0){
       return res.status(404).json({error : "No campaign_Name FOUND"});
       }
       res.status(200).json(getData)
    }catch(err){
     console.log(err.message);
     res.status(500).json({error : err.message});
    }
 } 
const maxViews = async(req,res)=>{
   try{
     const query = `SELECT *, views AS MaxViews 
      FROM \`sacred-catfish-415008.aruldb_dataset.fileData\`
      WHERE CAST(views AS INT64) = (SELECT MAX(CAST(views AS INT64)) FROM \`sacred-catfish-415008.aruldb_dataset.fileData\`)`
      
   
   const [views] = await bigquery.query(query)
   if(!views || views.length === 0){
      return res.status(404).json({error : 'data not found'})
   }
   res.status(200).json(views)
   }catch(err){
      res.status(500).json({error : err.message})  
   }
}

const minViews = async(req,res)=>{
   try{
      const query = `SELECT * FROM \`sacred-catfish-415008.aruldb_dataset.fileData\`WHERE CAST(views AS INT64) = (SELECT MIN(CAST(views AS INT64)) FROM \sacred-catfish-415008.aruldb_dataset.fileData\)`; 
   
   const [views] = await bigquery.query(query)
   if(!views || views.length === 0){
      return res.status(404).json({error : 'data not found'})
   }
   res.status(200).json(views)
   }catch(err){
      res.status(500).json({error : err.message})  
   }
}

const maxClicks = async(req,res)=>{
   try{
      const query = `SELECT * FROM \`sacred-catfish-415008.aruldb_dataset.fileData\`WHERE CAST(clicks AS INT64) = (SELECT MAX(CAST(clicks AS INT64)) FROM \sacred-catfish-415008.aruldb_dataset.fileData\)`; 
   
   const [clicks] = await bigquery.query(query)
   if(!clicks || clicks.length === 0){
      return res.status(404).json({error : 'data not found'})
   }
   res.status(200).json(clicks)
   }catch(err){
      res.status(500).json({error : err.message})  
   }
}

const minClicks = async(req,res)=>{
   try{
      const query = `SELECT * FROM \`sacred-catfish-415008.aruldb_dataset.fileData\`WHERE CAST(clicks AS INT64) = (SELECT MIN(CAST(clicks AS INT64)) FROM \sacred-catfish-415008.aruldb_dataset.fileData\)`; 
   
   const [clicks] = await bigquery.query(query)
   if(!clicks || clicks.length === 0){
      return res.status(404).json({error : 'data not found'})
   }
   res.status(200).json(clicks)
   }catch(err){
      res.status(500).json({error : err.message})  
   }
}

const maxAd_spend = async(req,res)=>{
   try{
      const query = `SELECT * FROM \`sacred-catfish-415008.aruldb_dataset.fileData\`WHERE CAST(Ad_spend AS FLOAT64) = (SELECT MAX(CAST(Ad_spend AS FLOAT64)) FROM \sacred-catfish-415008.aruldb_dataset.fileData\)`; 
   
   const [maxAd_spend] = await bigquery.query(query)
   if(!maxAd_spend || maxAd_spend.length === 0){
      return res.status(404).json({error : 'data not found'})
   }
   res.status(200).json(maxAd_spend)
   }catch(err){
      res.status(500).json({error : err.message})  
   }
}

const minAd_spend = async(req,res)=>{
   try{
      const query = `SELECT * FROM \`sacred-catfish-415008.aruldb_dataset.fileData\`WHERE CAST(Ad_spend AS FLOAT64) = (SELECT Min(CAST(Ad_spend AS FLOAT64)) FROM \sacred-catfish-415008.aruldb_dataset.fileData\)`; 
   
   const [maxAd_spend] = await bigquery.query(query)
    if(!maxAd_spend || maxAd_spend.length === 0){
      return res.status(404).json({error : 'data not found'})
   }
   res.status(200).json(maxAd_spend)
   }catch(err){
      res.status(500).json({error : err.message})  
   }
}

const uniqueCampaign = async(req,res)=>{
   try{
      const query = `SELECT 
      campaign_ID,
      (SELECT SUM(CAST(views AS INT64)) FROM \`sacred-catfish-415008.aruldb_dataset.fileData\` AS total_views WHERE total_views.campaign_ID = fileData.campaign_ID) AS total_views,
      (SELECT SUM(CAST(clicks AS INT64)) FROM \`sacred-catfish-415008.aruldb_dataset.fileData\` AS total_clicks WHERE total_clicks.campaign_ID = fileData.campaign_ID) AS total_clicks,
      (SELECT SUM(CAST(Ad_spend AS FLOAT64)) FROM \`sacred-catfish-415008.aruldb_dataset.fileData\` AS total_AdSpend WHERE total_AdSpend.campaign_ID = fileData.campaign_ID) AS total_AdSpend
  FROM 
      (SELECT DISTINCT campaign_ID FROM \`sacred-catfish-415008.aruldb_dataset.fileData\`) AS fileData`;
  
   
   const [result] = await bigquery.query(query)
    if(!result || result.length === 0){
      return res.status(404).json({error : 'data not found'})
   }
   res.status(200).json(result)
   }catch(err){
      res.status(500).json({error : err.message})  
   }
}


module.exports = {getdata,campaign_ID,Ad_Group_ID,FSN_ID,product_Name,campaign_Name,maxViews,minViews,maxClicks,minClicks,maxAd_spend,minAd_spend,uniqueCampaign}
