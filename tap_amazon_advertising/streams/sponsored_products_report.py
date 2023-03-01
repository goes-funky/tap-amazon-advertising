from dataclasses import dataclass

from tap_amazon_advertising.streams.base import ReportStream, ReportStreamV3

import singer
import json

LOGGER = singer.get_logger()  # noqa
@dataclass
class Configuration:
    adProduct:str
    groupBy:list
    columns:list
    reportTypeId:str
    timeUnit:str
    format:str

@dataclass
class DataBody:
    name:str
    startDate:str
    endDate:str
    configuration:Configuration


class BaseSponsoredProductsReportStream(ReportStreamV3):
    API_METHOD = 'GET'

    @property
    def recordType(self):
        raise RuntimeError("not implemented")

    @property
    def api_path(self):
        return '/reporting/reports'

    def get_body(self, date_start, date_end):
        dataConfiguration = Configuration(**{
                "adProduct": "SPONSORED_PRODUCTS",
                "groupBy": [],
                "columns": self.selected_columns,
                "reportTypeId": "spCampaigns",
                "timeUnit": "DAILY",
                "format": "GZIP_JSON"
            })
        dataBody = {
            "name":"",
            "startDate": date_start.strftime('%Y-%m-%d'),
            "endDate": date_end.strftime('%Y-%m-%d'),
            "configuration":dataConfiguration
        }
        body = DataBody(**dataBody)
        return body

    def get_stream_data(self, result, day):
        return [
            self.transform_record(record)
            for record in result
        ]


class SponsoredProductsReportProductAdsStream(BaseSponsoredProductsReportStream):
    TABLE = 'sponsored_products_report_product_ads'
    KEY_PROPERTIES = ['adId', 'day', 'profileId']

    @property
    def recordType(self):
        return "productAds"

    def get_body(self, day):
        return {
            "reportDate": day.strftime('%Y%m%d'),
            "metrics": ",".join([
                "campaignName",
                "campaignId",
                "adGroupName",
                "adGroupId",
                "impressions",
                "clicks",
                "cost",
                "campaignBudgetCurrencyCode",
                "advertisedAsin",
                # "sku", # Not supported for vendors?
                "purchases1d",
                "purchases7d",
                "purchases14d",
                "purchases30d",
                "",
                "purchasesSameSku7d",
                "purchasesSameSku14d",
                "purchasesSameSku30d",
                "unitsSoldClicks1d",
                "unitsSoldClicks7d",
                "unitsSoldClicks14d",
                "unitsSoldClicks30d",
                "sales1d",
                "sales7d",
                "sales14d",
                "sales30d",
                "attributedSalesSameSku1d",
                "attributedSalesSameSku7d",
                "attributedSalesSameSku14d",
                "attributedSalesSameSku30d",
            ])
        }


class SponsoredProductsReportCampaignsStream(BaseSponsoredProductsReportStream):
    TABLE = 'sponsored_products_report_campaigns'
    KEY_PROPERTIES = ['campaignId', 'date']

    @property
    def recordType(self):
        return "campaigns"

    def get_body(self, date_start, date_end):
        body = super().get_body(date_start,date_end)
        body.configuration.groupBy = ["campaign"]
        body.configuration.columns = [
            "impressions",
            "clicks",
            "cost",
            "purchases1d",
            "purchases7d",
            "purchases14d",
            "purchases30d",
            "purchasesSameSku1d",
            "purchasesSameSku7d",
            "purchasesSameSku14d",
            "purchasesSameSku30d",
            "unitsSoldClicks1d",
            "unitsSoldClicks7d",
            "unitsSoldClicks14d",
            "unitsSoldClicks30d",
            "sales1d",
            "sales7d",
            "sales14d",
            "sales30d",
            "attributedSalesSameSku1d",
            "attributedSalesSameSku7d",
            "attributedSalesSameSku14d",
            "attributedSalesSameSku30d",
            "unitsSoldSameSku1d",
            "unitsSoldSameSku7d",
            "unitsSoldSameSku14d",
            "unitsSoldSameSku30d",
            "kindleEditionNormalizedPagesRead14d",
            "kindleEditionNormalizedPagesRoyalties14d",
            "date",
            "campaignBiddingStrategy",
            "costPerClick",
            "clickThroughRate",
            "spend",
            "campaignName",
            "campaignId",
            "campaignStatus",
            "campaignBudgetAmount",
            "campaignBudgetType",
            "campaignRuleBasedBudgetAmount",
            "campaignApplicableBudgetRuleId",
            "campaignApplicableBudgetRuleName",
            "campaignBudgetCurrencyCode"
            ]
        return body


class SponsoredProductsReportAdGroupsStream(BaseSponsoredProductsReportStream):
    TABLE = 'sponsored_products_report_ad_groups'
    KEY_PROPERTIES = ['adGroupId', 'date']

    @property
    def recordType(self):
        return "adGroups"

    def get_body(self, date_start, date_end):
        body = super().get_body(date_start, date_end)
        body.configuration.groupBy = ["campaign","adGroup"]
        return body


class SponsoredProductsReportKeywordsStream(BaseSponsoredProductsReportStream):
    TABLE = 'sponsored_products_report_keywords'
    KEY_PROPERTIES = ['keywordId', 'day', 'profileId']

    @property
    def recordType(self):
        return "keywords"

    def get_body(self, day):
        return {
            "reportDate": day.strftime('%Y%m%d'),
            "metrics": ",".join([
                "campaignName",
                "campaignId",
                "keywordId",
                "keyword",
                "matchType",
                "impressions",
                "clicks",
                "cost",
                "purchases1d",
                "purchases7d",
                "purchases14d",
                "purchases30d",
                "purchasesSameSku1d",
                "purchasesSameSku7d",
                "purchasesSameSku14d",
                "purchasesSameSku30d",
                "unitsSoldClicks1d",
                "unitsSoldClicks7d",
                "unitsSoldClicks14d",
                "unitsSoldClicks30d",
                "sales1d",
                "sales7d",
                "sales14d",
                "sales30d",
                "attributedSalesSameSku1d",
                "attributedSalesSameSku7d",
                "attributedSalesSameSku14d",
                "attributedSalesSameSku30d",
            ])
        }


class SponsoredProductsReportTargetStream(BaseSponsoredProductsReportStream):
    TABLE = 'sponsored_products_report_targets'
    KEY_PROPERTIES = ['targetId', 'day', 'profileId']

    @property
    def recordType(self):
        return "targets"

    def get_body(self, day):
        return {
            "reportDate": day.strftime('%Y%m%d'),
            "metrics": ",".join(
                ['adGroupId', 'adGroupName', 'purchases14d', 'purchasesSameSku14d',
                 'purchases1d', 'purchasesSameSku1d', 'purchases30d',
                 'purchasesSameSku30d', 'purchases7d', 'purchasesSameSku7d',
                 'sales14d', 'attributedSalesSameSku14d', 'sales1d', 'attributedSalesSameSku1d',
                 'sales30d', 'attributedSalesSameSku30d', 'sales7d', 'attributedSalesSameSku7d',
                 'unitsSoldClicks14d', 'unitsSoldSameSku14d', 'unitsSoldClicks1d',
                 'unitsSoldSameSku1d', 'unitsSoldClicks30d', 'unitsSoldSameSku30d',
                 'unitsSoldClicks7d', 'unitsSoldSameSku7d', 'campaignBudgetAmount', 'campaignBudgetType',
                 'campaignId', 'campaignName', 'campaignStatus', 'clicks', 'cost', 'impressions', 'keywordId',
                 'keyword', 'targeting', 'keywordType']

            )
        }


class SponsoredProductsReportSearchTermsKeywordStream(SponsoredProductsReportKeywordsStream):
    TABLE = 'sponsored_products_report_search_terms_keyword'
    KEY_PROPERTIES = ['keywordId', 'day', 'profileId']

    def get_body(self, day):
        body = super().get_body(day)
        body["segment"] = "query"
        return body


class SponsoredProductsReportSearchTermsTargetStream(SponsoredProductsReportTargetStream):
    TABLE = 'sponsored_products_report_search_terms_target'
    KEY_PROPERTIES = ['targetId', 'day', 'profileId']

    def get_body(self, day):
        body = super().get_body(day)
        body["segment"] = "query"
        return body


class SponsoredProductsReportPlacement(BaseSponsoredProductsReportStream):
    TABLE = "sponsored_products_report_placement"
    KEY_PROPERTIES = ['campaignId', 'day', 'profileId']

    @property
    def recordType(self):
        return "campaigns"

    def get_body(self, day):
        return {
            "reportDate": day.strftime('%Y%m%d'),
            "segment": "placement",
            "metrics": ",".join(['campaignApplicableBudgetRuleId', 'campaignApplicableBudgetRuleName', 'purchases14d',
                                 'purchasesSameSku14d', 'purchases1d',
                                 'purchasesSameSku1d', 'purchases30d',
                                 'purchasesSameSku30d', 'purchases7d',
                                 'purchasesSameSku7d', 'sales14d',
                                 'attributedSalesSameSku14d', 'sales1d', 'attributedSalesSameSku1d',
                                 'sales30d', 'attributedSalesSameSku30d', 'sales7d',
                                 'attributedSalesSameSku7d', 'unitsSoldClicks14d',
                                 'unitsSoldSameSku14d', 'unitsSoldClicks1d',
                                 'unitsSoldSameSku1d', 'unitsSoldClicks30d',
                                 'unitsSoldSameSku30d', 'unitsSoldClicks7d',
                                 'unitsSoldSameSku7d', 'bidPlus', 'campaignBudgetAmount',
                                 'campaignBudgetType', 'campaignId', 'campaignName', 'campaignRuleBasedBudgetAmount',
                                 'campaignStatus', 'clicks', 'cost', 'campaignBudgetCurrencyCode', 'impressions']
                                )
        }


class __SponsoredProductsReportASIN(BaseSponsoredProductsReportStream):

    @property
    def recordType(self):
        return "asins"

    def get_body(self, day):
        return {
            "reportDate": day.strftime('%Y%m%d'),
            "metrics": ",".join(['adGroupId', 'adGroupName', 'advertisedAsin', 'salesOtherSku14d',
                                 'salesOtherSku1d', 'salesOtherSku30d', 'salesOtherSku7d',
                                 'unitsSoldClicks14d', 'unitsSoldOtherSku14d',
                                 'unitsSoldClicks1d', 'unitsSoldOtherSku1d',
                                 'unitsSoldClicks30d', 'unitsSoldOtherSku30d',
                                 'unitsSoldClicks7d', 'unitsSoldOtherSku7d', 'campaignId',
                                 'campaignName', 'campaignBudgetCurrencyCode', 'matchType', 'purchasedAsin',
                                 'sku']
                                ),
            "campaignType": "sponsoredProducts"
        }


class SponsoredProductsReportASINKeyword(__SponsoredProductsReportASIN):
    TABLE = "sponsored_products_report_asins_keywords"
    KEY_PROPERTIES = ['asin', 'day', 'profileId']

    def get_body(self, day):
        b = super().get_body(day)
        b["metrics"] = b["metrics"] + ",keywordId,keyword"

        return b


class SponsoredProductsReportASINTarget(__SponsoredProductsReportASIN):
    TABLE = "sponsored_products_report_asins_targets"
    KEY_PROPERTIES = ['asin', 'day', 'profileId']

    def get_body(self, day):
        b = super().get_body(day)
        b["metrics"] = b["metrics"] + ",keywordId,keyword,targeting,keywordType"
        return b