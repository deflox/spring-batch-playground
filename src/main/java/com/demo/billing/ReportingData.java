package com.demo.billing;

import com.demo.common.BillingData;

public record ReportingData(BillingData billingData, double billingTotal){
}
