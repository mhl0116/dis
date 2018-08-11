function isDict(v) {
    return (typeof v==='object' && v!==null && !(v instanceof Array) && !(v instanceof Date));
}

function preprocessJSON(json) {
    for (var i = 0; i < json.length; i++) {
        console.log(json[i]);
        console.log(isDict(json[i]));
        if (isDict(json[i]) && ("timestamp" in json[i])) {
            json[i]["timestamp"] = new Date(json[i]["timestamp"]*1000);
        }
    }
    
    return json;
}
function prettyJSON(elem, json) {
    json = preprocessJSON(json);
    var node = new PrettyJSON.view.Node({ 
        el:elem,
        data:json,
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
    $("#result_container").show();
    $("#query").siblings("i").removeClass("loading");
    var secs = (new Date().getTime()-t0)/1000;
    var which = "success";
    if (secs > 3.0) { which = "warning"; }
    if (secs > 10.0) { which = "error"; }
    $("#timing").html(`loaded in
        <span class="label label-${which} label-rounded">${secs}</span>
        seconds`);
    $("#query").blur();
}
function doSubmit(data) {
    if ($("#query").is(":invalid")) {
        return;
    }
    $("#query").siblings("i").addClass("loading");
    $("#result_container").hide();
    t0 = new Date().getTime();
    console.log("data");
    console.log(data);
    $.get("handler.py", data)
        .done(function(response) {})
        .always(handleResponse);
}

function initHide() {
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
    $("#aqueryurl").addClass('btn-primary').delay(75).queue(function(next){
         $(this).removeClass('btn-primary').dequeue();
    });
}

function getQueryCLI() {
    var data = {};
    $.each($("#main_form").serializeArray(), function (i, field) { data[field.name] = field.value || ""; });
    extra = "--detail "
    if ("short" in data) extra = "";
    var clicmd = "dis_client.py -t "+data["type"]+" "+extra+"\""+data["query"]+"\"";
    console.log(clicmd);
    copyToClipboard(clicmd)
    $("#aquerycli").addClass('btn-primary').delay(75).queue(function(next){
         $(this).removeClass('btn-primary').dequeue();
    });
}


$(function(){

    $.ajaxSetup({timeout: 15000});

    $("#select_type").change(function(e) {
        console.log(e);
        var val = e.target.value;
        console.log(val);
        if (["snt","dbs","sites"].includes(val)) {
            $("#query").removeAttr("pattern");
            $("#query").removeAttr("oninvalid");
            console.log("not setting pattern");
            console.log($("#query"));
        } else {
            $("#query").attr("pattern", "/.+/.+/[^/]+");
            $("#query").attr("title", 'Need 3 slashes in dataset name');
            console.log("yep... setting pattern");
        }
        console.log( "Handler for .change() called." );
    });
    $("#select_type").trigger("change");

    initHide();
    $("#submit_button").click(submitQuery);

    // if page was loaded with a parameter for search, then simulate a search
    if(window.location.href.indexOf("?") != -1) {
        // parse and sanitize
        var search = location.search.substring(1);
        var query_dict = search?JSON.parse('{"' + search.replace(/&/g, '","').replace(/=/g,'":"') + '"}', function(key, value) { return key===""?value:decodeURIComponent(value) }):{};

        var query = query_dict["query"];
        query = query.replace(/\+/g, ' ');
        query_dict["type"] = query_dict["type"] || "basic";

        // check or uncheck short box, pick dropdown item, and fill in query box
        document.getElementById("checkboxshort").checked = Boolean(query_dict["short"]);
        $("#select_type").val(query_dict["type"]);
        $("#select_type").trigger("change");
        $("#query").val(query);

        // submit
        console.log(query_dict);
        submitQuery()
    }

    $( "#rocket" ).click(function() {
        $( "#rocket" ).animate({
            left: "+=50",
            top: "-=50"
        }, 1500);
    });

    
});

// vimlike incsearch: press / to focus on search box
$(document).keydown(function(e) {
    var target = $(event.target);
    // console.log(e.keyCode);
    if (!target.is("#query") && !target.is("#select_type")) {
        if(e.keyCode == 191) {
            // / focus search box
            e.preventDefault();
            $("#query").focus().select();
        }
        // y to copy url
        // Y to copy cli command
        if(e.keyCode== 89) {
            if (e.shiftKey) {
                getQueryCLI();
            } else {
                getQueryURL();
            }
        }
    }
});
