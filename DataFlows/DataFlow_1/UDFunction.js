function transform(line) {
    var values = line.split(';');
    
    var obj = new Object();
    obj.Producer_name = values[0];
    obj.Number_of_Followers_on_Twitter = values[1];
    obj.keyword = values[2];
    obj.Tweet_ID = values[3];
    obj.Date = values[4];
    obj.Likes = values[5];
    obj.Retweets = values[6];
    obj.Text = values[7];
    obj.Original_Language = values[8];
    obj.Location = values[9];
    obj.Posted_by = values[10];
    obj.User_Follower = values[11];
    obj.User_Following = values[12];
    var jsonString = JSON.stringify(obj);
    
    return jsonString;
    }