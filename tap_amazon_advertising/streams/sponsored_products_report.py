from dataclasses import dataclass

from tap_amazon_advertising.streams.base import ReportStreamV3

import singer

LOGGER = singer.get_logger()  # noqa
@dataclass
class Configuration:
    adProduct:str
    groupBy:list
    columns:list
    reportTypeId:str
    timeUnit:str
    format:str
    filters:list

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
                "format": "GZIP_JSON",
                "filters": []
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


class SponsoredProductsReportCampaignsStream(BaseSponsoredProductsReportStream):
    TABLE = 'sponsored_products_report_campaigns'
    KEY_PROPERTIES = ['campaignId', 'date']

    @property
    def recordType(self):
        return "campaigns"

    def get_body(self, date_start, date_end):
        body = super().get_body(date_start,date_end)
        body.configuration.groupBy = ["campaign"]
        return body

class SponsoredProductsReportCampaignsPlacementStream(BaseSponsoredProductsReportStream):
    TABLE = 'sponsored_products_report_campaign_placement'
    KEY_PROPERTIES = ['campaignId', 'date']

    @property
    def recordType(self):
        return "campaigns"

    def get_body(self, date_start, date_end):
        body = super().get_body(date_start,date_end)
        body.configuration.groupBy = ["campaign","campaignPlacement"]
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

class SponsoredProductsReportTargetExpressionStream(BaseSponsoredProductsReportStream):
    TABLE = 'sponsored_products_report_targets_expressions'
    KEY_PROPERTIES = ['adGroupId','campaignId','keywordId', 'date']

    def get_body(self, date_start, date_end):
        body = super().get_body(date_start=date_start, date_end=date_end)
        filters = {
                    "field": "keywordType",
                    "values": [
                    "TARGETING_EXPRESSION",
                    "TARGETING_EXPRESSION_PREDEFINED"
                    ]
                }
        body.configuration.filters = [filters]
        body.configuration.groupBy = ["targeting"]
        body.configuration.reportTypeId = "spTargeting"
        return body

class SponsoredProductsReportTargetKeywordsStream(BaseSponsoredProductsReportStream):
    TABLE = 'sponsored_products_report_targets_keywords'
    KEY_PROPERTIES = ['adGroupId','campaignId','keywordId', 'date']

    def get_body(self, date_start, date_end):
        body = super().get_body(date_start=date_start, date_end=date_end)
        filters = {
                    "field": "keywordType",
                    "values": [
                    "BROAD",
                    "PHRASE",
                    "EXACT"
                    ]
                }
        body.configuration.filters = [filters]
        body.configuration.groupBy = ["targeting"]
        body.configuration.reportTypeId = "spTargeting"
        return body

class SponsoredProductsReportSearchTermsKeywordStream(BaseSponsoredProductsReportStream):
    TABLE = 'sponsored_products_report_search_terms_keywords'
    KEY_PROPERTIES = ['adGroupId','campaignId','keywordId', 'date']

    def get_body(self, date_start, date_end):
        body = super().get_body(date_start=date_start, date_end=date_end)
        filters =  {
                    "field": "keywordType",
                    "values": [
                    "BROAD",
                    "PHRASE",
                    "EXACT"
                    ]
                }
        body.configuration.filters = [filters]
        body.configuration.groupBy = ["searchTerm"]
        body.configuration.reportTypeId = "spSearchTerm"
        return body


class SponsoredProductsReportSearchTermsExpressionStream(BaseSponsoredProductsReportStream):
    TABLE = 'sponsored_products_report_search_terms_expressions'
    KEY_PROPERTIES = ['adGroupId','campaignId','keywordId', 'date']

    def get_body(self, date_start, date_end):
        body = super().get_body(date_start=date_start, date_end=date_end)
        filters = {
                    "field": "keywordType",
                    "values": [
                    "TARGETING_EXPRESSION",
                    "TARGETING_EXPRESSION_PREDEFINED"
                    ]
                }
        body.configuration.filters = [filters]
        body.configuration.groupBy = ["searchTerm"]
        body.configuration.reportTypeId = "spSearchTerm"
        return body

class SponsoredProductsReportAdvertiserStream(BaseSponsoredProductsReportStream):
    TABLE = 'sponsored_products_report_advertiser'
    KEY_PROPERTIES = ['advertisedAsin','campaignId','adId', 'date']

    def get_body(self, date_start, date_end):
        body = super().get_body(date_start=date_start, date_end=date_end)
        body.configuration.groupBy = ["advertiser"]
        body.configuration.reportTypeId = "spAdvertisedProduct"
        return body

class SponsoredProductsReportAsinStream(BaseSponsoredProductsReportStream):
    TABLE = 'sponsored_products_report_asin'
    KEY_PROPERTIES = ['advertisedAsin','campaignId', 'date']

    def get_body(self, date_start, date_end):
        body = super().get_body(date_start=date_start, date_end=date_end)
        body.configuration.groupBy = ["asin"]
        body.configuration.reportTypeId = "spPurchasedProduct"
        return body