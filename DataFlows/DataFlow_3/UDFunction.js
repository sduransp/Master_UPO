function transform(line) {
    var values = line.split(';');

    var obj = new Object();
    obj.OfferDate = values[0];
    obj.ProducerName = values[1];
    obj.ProductName = values[2];
    obj.BottleSize = values[3];
    obj.BottleSizeEquiv = values[4];
    obj.Vintage = values[5];
    obj.MerchantName = values[6];
    obj.MerchantCountry = values[7];
    obj.MerchantState = values[8];
    obj.MerchantCity = values[9];
    obj.CurrencyCode = values[4];
    obj.LocalPrice = values[10];
    obj.PriceInEur = values[11];
    obj.TaxStatus = values[12];
    obj.LocalPriceExTax = values[13];
    obj.PriceInEurTaxExcl = values[14];
    obj.PriceInEURTaxExclPer750ml = values[15];
    obj.FindPage = values[16];
    obj.DeepLink = values[17];
    obj.BusinessType = values[18];
    var jsonString = JSON.stringify(obj);

    return jsonString;
    }