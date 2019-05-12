var timer;
var debugMode = false;

function isDict(v) {
    return (typeof v==='object' && v!==null && !(v instanceof Array) && !(v instanceof Date));
}

function preprocessJSON(json) {
    for (var i = 0; i < json.length; i++) {
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
var latestresult = {};
function handleResponse(response) {
    console.log(response);
    if (response && ("payload" in response)) {
        if(response["status"] == "success") {
            if (debugMode) {
                prettyJSON($("#result"), response);
                latestresult = response;
            } else {
                prettyJSON($("#result"), response["payload"]);
                latestresult = response["payload"];
            }
        } else {
            prettyJSON($("#result"), response);
            latestresult = {};
        }
    }
    $("#result_container").show();
    $("#query").siblings("i").removeClass("loading");
    $("#result").addClass('highlight').delay(150).queue(function(next){
         $(this).removeClass('highlight').dequeue();
    });
    AJS.$('#submit_button').each(function() {
        this.idle();
    });
    $(".string").mousedown(
        function(e) {
            // middle mouse button
            if(e.which == 2) {
                // double replace instead of regex (replace(/"/g,"")) because of vim highlighting
                console.log(e.target); $("#query").val(e.target.innerHTML.replace('"',"").replace('"',"").trim());
            }
        }
    )

    var secs = (new Date().getTime()-t0)/1000;
    var which = "success";
    if (secs > 3.0) { which = "moved"; }
    if (secs > 30.0) { which = "error"; }
    $("#timing").html(`loaded in
        <span class="aui-lozenge aui-lozenge aui-lozenge-${which} label-rounded">${secs}</span>
        seconds`);
}
function doSubmit(data) {
    if ($("#query").is(":invalid")) {
        return;
    }
    $("#query").siblings("i").addClass("loading");
    t0 = new Date().getTime();
    console.log(data);
    $.get("http://uafino.physics.ucsb.edu:50010/dis/serve", data)
        .done(function(response) {})
        .always(handleResponse);
}

function submitQuery() {
    var data = {};
    $.each($("#main_form").serializeArray(), function (i, field) { data[field.name] = field.value || ""; });
    if (data["short"] == "short") data["short"] = true;
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
    var myFlag = AJS.flag({
        type: 'success',
        body: 'Copied URL to clipboard',
        close: 'auto'
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

    AJS.$("#checkboxshort").tooltip();
    AJS.$("#aqueryurl").tooltip();
    AJS.$("#ahelp").tooltip();

    $("#aqueryurl").click(function(e) {
        e.preventDefault();
    });
    $("#ahelp").click(function(e) {
        e.preventDefault();
    });

    $.ajaxSetup({timeout: 45000});

    $("#select_type").trigger("change");
    AJS.$("#main_form").submit(function(e) {
        e.preventDefault();
        submitQuery();
    });
    AJS.$(document).on('click', '#submit_button', function() {
        this.busy();
    });

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
        submitQuery();
    }

    $( "#rocket" ).click(function() {
        $( "#rocket" ).animate({
            left: "+=50",
            top: "-=50"
        }, 1500);
    });

    AJS.whenIType('/').execute(function() {
        $("#query").focus().select();
    });
    AJS.whenIType('y').execute(function() {
        getQueryURL();
    });
    AJS.whenIType('dbg').execute(function() {
        var myFlag = AJS.flag({
            type: 'info',
            body: "Turning debug "+(debugMode ? "OFF" : "ON"),
            close: 'auto'
        });
        debugMode ^= true;
        console.log(debugMode);
    });
    AJS.whenIType('ccc').execute(function() {
        $.get("http://uafino.physics.ucsb.edu:50010/dis/clearcache")
            .always(function(response) {
                console.log(response);
                var myFlag = AJS.flag({
                    type: 'info',
                    body: "Clearing cache: "+JSON.stringify(response),
                    close: 'auto'
                });
            });
    });

});

