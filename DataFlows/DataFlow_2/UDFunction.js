function transform(line) {
    var values = line.split(';');

    var obj = new Object();
    obj.Date = values[0];
    obj.Keyword = values[1];
    obj.text = values[2];
    obj.sentiment = values[3];
    obj.likes = values[4];
    obj.user_followers = values[5];
    var jsonString = JSON.stringify(obj);

    return jsonString;
    }