var timer;
var liveSearch = false;

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

var tableDrawn = false;
function toggleTable() {
    if (tableDrawn) {
        prettyJSON($("#result"), latestresult);
        $("#result").removeClass("tabulator");
    } else {
        var table = new Tabulator("#result", {
            data:latestresult,
            // height:"80%", // this messes up height when switching back from table
            layout:"fitColumns",
            columns:[
                {title:"dataset_name", field:"dataset_name",minWidth:600},
                {title:"cms3tag", field:"cms3tag",sorter:"string",minWidth:100},
                {title:"age", field:"timestamp", formatter:"datetimediff", formatterParams:{humanize:true}, sorter:"date"},
                {title:"nevents", field:"nevents_out"},
                {title:"xsec", field:"xsec"},
                {title:"kfact", field:"kfactor"},
                {title:"location", field:"location",minWidth:300},
            ]
        });
    }
    tableDrawn ^= true;
    console.log(tableDrawn);
}

var t0;
var latestresult = {};
function handleResponse(response) {
    console.log(response);
    if (response && ("payload" in response)) {
        if(response["status"] == "success") {
            prettyJSON($("#result"), response["payload"]);
            latestresult = response["payload"];
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
    if (secs > 3.0) { which = "warning"; }
    if (secs > 30.0) { which = "error"; }
    $("#timing").html(`loaded in
        <span class="label label-${which} label-rounded">${secs}</span>
        seconds`);
    // $("#query").blur();
}
function doSubmit(data) {
    if ($("#query").is(":invalid")) {
        return;
    }
    $("#query").siblings("i").addClass("loading");
    // $("#result_container").hide();
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

function toggleLive() {
    liveSearch ^= true;
    console.log(liveSearch);
}

$(function(){

    $.ajaxSetup({timeout: 45000});

    $("#select_type").change(function(e) {
        var val = e.target.value;
        console.log(val);
        if (["snt","dbs","sites"].includes(val)) {
            $("#query").removeAttr("pattern");
            $("#query").removeAttr("oninvalid");
        } else {
            $("#query").attr("pattern", "/.+/.+/[^/]+");
            $("#query").attr("title", 'Need 3 slashes in dataset name');
        }
    });
    $("#select_type").trigger("change");
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
        liveSearch = Boolean(query_dict["live"]);
        document.getElementById("checkboxlive").checked = liveSearch;
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

    var lastVal = "";
    $("#query").keyup(function(e) {
        if (!liveSearch) return;
        if (this.value == lastVal) return;
        if (lastVal == "") {
            lastVal = this.value;
            return;
        } else {
            lastVal = this.value;
        }
        clearTimeout(timer);
        timer = setTimeout(function() {
            submitQuery();
        }, 400);
    });

    
});

// vimlike incsearch: press / to focus on search box
$(document).keydown(function(e) {
    var target = $(event.target);
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
