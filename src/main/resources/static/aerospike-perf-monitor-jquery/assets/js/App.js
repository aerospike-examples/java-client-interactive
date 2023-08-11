$(function() {
    let $latencyGraph = $("#latencyGraph");
    let $throughputGraph = $("#throughputGraph");
    let latencyGraph = StatisticsGraph($latencyGraph, "Aggregate", "LATENCY");
    let throughputGraph = StatisticsGraph($throughputGraph, "", "THROUGHPUT");

    let now = 0;
    console.log("starting up");

    $.ajax({
        url: '/api/getRecordingType',
        dataType: 'json',
        success: function(data, xhr) {
            console.log(data);
            let type = data.type;
            let types = type.split(",");
            for (thisType of types) {
                if (thisType === "PER_CALL") {
                    $("#individualCallsEnabled").prop("checked", true);
                }
                else if (thisType === "AGGREGATE") {
                    $("#aggregateCallsEnabled").prop("checked", true);
                }
            }

        },
        error: function(data) {
            console.log(data);
        }
     });

    setInterval(() => {
        $.ajax({
			url: '/api/samples',
			type: 'GET', 
			data: {since: now},
			dataType: 'json',
            success: function(data) {
                let aggregateData = data.aggregatingSamples;
                latencyGraph.update(aggregateData);
                throughputGraph.update(aggregateData);
            },
            error: (error) => {
                console.log(error);
            }
        });
    }, 1000);
    
    $(".run-command").on("click", function() {
		let command = $(this).data("command");
		$.ajax({
			url: '/api/callback',
			data: {command : command},
			success: function(data, xhr) {
				console.log(data, xhr);
			},
			error: function(data) {
				console.log("Error - ", data)
			}
		});
	});

    function sendRecordingTypeChange() {
        let enabledTypes = [];
        if ($("#individualCallsEnabled").prop("checked")) {
            enabledTypes.push("PER_CALL");
        }
        if ($("#aggregateCallsEnabled").prop("checked")) {
            enabledTypes.push("AGGREGATE");
        }
        $.ajax({
            url: '/api/recordingType',
            data: {type: enabledTypes.join(",")},
            dataType: 'json',
            success: function(data, xhr) {
            }
        });
    }

    $(".form-check-input").on("change", function() {
        console.log(this);
        let $this = $(this);
        let checked = $this.prop("checked");
        let id = $this.prop("id");
        if (id === "enabledControl") {
            $.ajax({
                url: '/api/enable',
                type: 'GET',
                data: {enabled : checked},
                dataType: 'json',
                success: function(data) {
                    console.log(data);
                    $(".sub-control").prop("disabled", !checked);
                }
            })
        }
        if (id === "individualCallsEnabled" || id === "aggregateCallsEnabled") {
            sendRecordingTypeChange();
        }
        else if ($this.hasClass("individual-type")) {
            // Options has to be a string, not an object
            let options = "";
            $(".individual-type").each(function() {
                let self = $(this);
                if (options.length > 0) {
                    options += ",";
                }
                options += self.prop("id") + ":" + self.prop("checked");
            });
            $.ajax({
                url: '/api/typeOptions',
                data: {type:"PER_CALL", options: options},
                dataType: 'json',
                success: function(data) {
                    console.log(data);
                }
            })

        }
    })
});