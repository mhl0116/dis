function prettyJSON(elem, json) {
    var node = new PrettyJSON.view.Node({ 
        el:elem,
        data:json
    });
    node.expandAll();
}

var t0;
function handleResponse(response) {
    console.log(response);
    if (response && ("response" in response)) {
        if(response["response"]["status"] == "success" && response["response"]["warning"].length == 0) {
            prettyJSON($("#result"), response["response"]["payload"]);
        } else {
            prettyJSON($("#result"), response["response"]);
        }
    }
    $("#loading_animation").hide();
    $("#result_container").show();
    $(".timing").html("loaded in " + (new Date().getTime()-t0)/1000 + " seconds");
}
function doSubmit(data) {
    $("#loading_animation").show();
    // $("#query_container").show();
    // prettyJSON($('#query'), data);

    // var cli_str = "dis_client.py ";
    // if(data["type"] != "basic") cli_str += "-t " + data["type"] + " ";
    // if(data["short"] != "short") cli_str += "--detail ";
    // cli_str += '"' + data["query"] + '"';
    // $('#clisyntax').html(cli_str);

    $("#result_container").hide();
    t0 = new Date().getTime();
    $.get("handler.py", data)
        .done(function(response) {})
        .always(handleResponse);
}

function initHide() {
    // $("#query_container").hide();
    $("#result_container").hide();
    $("#loading_animation").hide();
}

function submitQuery() {
    var data = {};
    $.each($("#main_form").serializeArray(), function (i, field) { data[field.name] = field.value || ""; });
    console.log(data);
    doSubmit(data);
}

function copyToClipboard(text) {
    var $temp = $("<input>");
    $("body").append($temp);
    $temp.val(text).select();
    document.execCommand("copy");
    $temp.remove();
}

function getQueryURL() {
    var data = {};
    $.each($("#main_form").serializeArray(), function (i, field) { data[field.name] = field.value || ""; });
    var queryURL = "http://"+location.hostname+location.pathname+"?"+($.param(data));
    queryURL = queryURL.replace("index.html","");
    console.log(queryURL);
    copyToClipboard(queryURL)
}


$(function(){

    initHide();
    $(".submit_button").click(submitQuery);

    // if page was loaded with a parameter for search, then simulate a search
    if(window.location.href.indexOf("?") != -1) {
        // parse and sanitize
        var search = location.search.substring(1);
        var query_dict = search?JSON.parse('{"' + search.replace(/&/g, '","').replace(/=/g,'":"') + '"}', function(key, value) { return key===""?value:decodeURIComponent(value) }):{};

        var query = query_dict["query"];
        query = query.replace(/\+/g, ' ');
        query_dict["type"] = query_dict["type"] || "basic";

        // check or uncheck short box, pick dropdown item, and fill in query box
        document.getElementById("short").checked = Boolean(query_dict["short"]);
        $("#select_type").val(query_dict["type"])
        $("#query").val(query);

        // submit
        console.log(query_dict);
        submitQuery()
    }
    
});
