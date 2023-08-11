function StatisticsGraph($location, timingMetricNm, timingType) {
    const MIN = 0;
    const AVG = 1;
    const MAX = 2;

    const margin = 50;
    let width = 800 - (margin * 2);
    let height = 600 - (margin * 2);
    let duration = 3*60*1000;

    var self = this;
    let $root = $location;
    var nativeElement = $root.get(0);
    let data = [];
    let svg;
    let margins = {top: 30, left: 50, right: 20, bottom: 50};
    let xScale;
    let xAxis;
    let yScale;
    let yAxis;
    let $xAxis;
    let $yAxis;
    let area;
    let line;
    let lineMin;
    let lineMax;
    let $area;
    let $data;
    let $dataMin;
    let $dataMax;
    let $tooltip;
    let minTime = 0;
    let visibilities = {0 : true, 1 : true, 2: true};
    let dateFormatter = d3.timeFormat('%H:%M:%S');
    let timingMetricName = timingMetricNm;
    let timingData = [];
  
    const construct = () => {
        let interval = timingMetricName === 'Aggregate' ? 1200 : 0;
        setTimeout( () => {
          width = nativeElement.offsetWidth;
          height = nativeElement.offsetHeight;
          console.log(width, height);
          console.log(window.devicePixelRatio);
          console.log(nativeElement.clientWidth);
          console.log(nativeElement.scrollWidth);
    
          if (timingType == "LATENCY") {
            visibilities[MAX] = false;
          }
          createSvg();
          defineHeading();
          defineLabels();
          defineAxes();
          defineLegend();
          update();
        }, interval);

        // obs = new ResizeObserver((entries) => {
        //     this.zone.run(() => {
        //         console.log(entries)
        //         for (let entry of entries) {
        //         const cr = entry.contentRect;
        //         console.log(`Element size: ${cr.width}px x ${cr.height}px`)
        //         }
        //     })
        // });
        // obs.observe(nativeElement);
    }

    const createSvg = () => {
        svg = d3.select(nativeElement)
          .append("svg")
          .attr("width", "100%")
          .attr("height", "100%")
          .attr("viewBox", "0 0 " + width + " " + height)
          .attr("preserveAspectRatio", 'xMinYMin');
    }
    
    const getHeadingString = () => {
        // if (timingMetricName) {
        //     return (timingType == "LATENCY" ? "Latency " : "Throughput ") + "(" + timingMetricName + ")";
        // }
        // else {
            return (timingType == "LATENCY" ? "Latency " : "Throughput ");
        // }
    }
    
    const changeTitle = (newTitle) => {
        timingMetricName = newTitle;
        if (svg) {
            svg.select('.chartTitle').text(getHeadingString());
        }
    }

    const defineHeading = () => {
        var xLabel = svg.append('g').attr('class', 'xLabel heading');
        let heading = getHeadingString();
        xLabel.append('text')
          .attr('class', 'chartTitle')
          .attr('text-anchor', 'middle')
          .attr('x', width/2)
          .attr('y', 20)
          .text(heading)
          .attr('font-size', '1.5em');
    }
    
    const defineLegend = () => {
        let boxHeight = 25;
        let boxWidth = 250;
        let xLabel = svg.append('g')
            .attr('class', 'legend')
            .attr('width', boxWidth)
            .attr('height', boxHeight)
            // .attr('transform', 'translate(10,10)')
            .attr('transform', 'translate(' +(width-260) + ',0)');
        if (width < 600) {
          xLabel.attr('visibility', 'hidden');
        }
        let box = xLabel.append('rect')
            .attr('x', 0)
            .attr('y', 1)
            .attr('width', boxWidth)
            .attr('height', boxHeight)
            .attr('stroke', 'white')
            .attr('fill', 'none');
    
        let labels = [];
        if (timingType =="LATENCY") {
          labels = ["Min", "Avg", "Max"];
        }
        else {
          labels = ["Failed", "Success", "Total"];
        }
        for (let i = 0; i < labels.length; i++) {
          let startX = (boxWidth / labels.length) * i;
          let classType = (i == 0) ? 'lineMin' : (i == 1) ? 'line' : 'lineMax';
          xLabel.append("line") 
            .attr("x1", startX + 12)
            .attr("y1", boxHeight/2)      
            .attr("x2", startX + 30) 
            .attr("y2", boxHeight/2) 
            .attr('class', 'data ' + classType);
          let textItem = xLabel.append('text')
            .attr('class', 'legendLabel ' + classType)
            .attr('text-anchor', 'start')
            .attr('x', startX + 34)
            .attr('y', 18)
            .attr('font-size', '1em')
            .attr('fill', 'white')
            .text(labels[i])
            .on("click", (evt) => { 
              let thisObj = d3.select(evt.srcElement);
              toggleVisibility(AVG, thisObj);
              toggleVisibility(MIN, thisObj);
              toggleVisibility(MAX, thisObj);
            });
    
          // If we need to default a visibility to false, set it true then toggle it to false.
          if (!visibilities[i]) {
            visibilities[i] = true;
            toggleVisibility(i, textItem);
          }
        }
    }

    const toggleVisibility = (type, element) => {
        let classString;
        switch (type) {
          case AVG: classString = "line"; break;
          case MIN: classString = "lineMin"; break;
          case MAX: classString = "lineMax"; break;
        }
        let isThisElement = element.classed(classString);
        if (isThisElement) {
          let newOpacity;
          if (visibilities[type]) {
            element.style("opacity", 0.5);
            newOpacity = 0;
          }
          else {
            element.style("opacity", 1);
            newOpacity = 1;
          }
          svg.select("path.data."+classString).style("opacity", newOpacity);
          visibilities[type] = !visibilities[type];
        }
    }
    
    const defineLabels = () => {
        var xLabel = svg.append('g').attr('class', 'xLabel');
        xLabel.append('text')
          .attr('class', 'chartTitle')
          .attr('x', width/2)
          .attr('y', height - 10)
          .text('Time')
          .attr('font-size', '1em');
    
        var yLabel = svg.append('g').attr('class', 'xLabel');
        yLabel.append('text')
          .attr('class', 'chartTitle')
          .attr('text-anchor', 'middle')
          .attr('transform', 'rotate(-90)')
          .attr('font-size', '1em')
          .attr('x', -margins.top-160)
          .attr('y', -margins.left+65)
          .text(timingType == 'LATENCY' ? 'Latency (ms)' : 'Count');
      }
    
      const defineAxes = () => {
        xScale = d3.scaleTime().range([margins.left, width - margins.right]);
        xAxis = d3.axisBottom(xScale)
            .tickFormat(d3.timeFormat('%H:%M:%S'))
            .ticks(10)
            .tickSizeInner(-height + margins.bottom + margins.top)
            .tickSizeOuter(6)
            .tickPadding(5);
    
        yScale = d3.scaleLinear().range([height - margins.bottom, margins.top]);
        yAxis = d3.axisLeft(yScale)
            .tickFormat(d3.format('.2s'))
            .tickSizeInner(-width + margins.left + margins.right)
            .tickSizeOuter(5)
            .tickPadding(8);
    
        if (timingType == "LATENCY") {
          line = d3.line()
            // .curve(d3.curveBasis)
            .x((d, i) => xScale(getX(d)))
            .y((d, i) => yScale(getY(d)));
    
          lineMax = d3.line()
            // .curve(d3.curveBasis)
            .x((d, i) => xScale(getX(d)))
            .y((d, i) => yScale(getMaxY(d)));
    
          lineMin = d3.line()
            // .curve(d3.curveBasis)
            .x((d, i) => xScale(getX(d)))
            .y((d, i) => yScale(getMinY(d)));
    
          area = d3.area()
            // .curve(d3.curveBasis)
            .x((d, i) => xScale(getX(d)))
            .y0(height-margins.bottom)
            .y1((d, i) => yScale(getY(d)));
        }
        else {
          line = d3.line()
            // .curve(d3.curveBasis)
            .x((d, i) => xScale(getX(d)))
            .y((d, i) => yScale(getSuccessfulTxns(d)));
    
          lineMax = d3.line()
            // .curve(d3.curveBasis)
            .x((d, i) => xScale(getX(d)))
            .y((d, i) => yScale(getTotalTxns(d)));
    
          lineMin = d3.line()
            // .curve(d3.curveBasis)
            .x((d, i) => xScale(getX(d)))
            .y((d, i) => yScale(getFailedTxns(d)));
    
          area = d3.area()
            .curve(d3.curveBasis)
            .x((d, i) => xScale(getX(d)))
            .y0(height-margins.bottom)
            .y1((d, i) => yScale(getY(d)));
        }
    
        $xAxis = svg.append('g').attr('class', 'x axis')
          .attr('transform', `translate(0, ${height-margins.bottom})`)
          .call(xAxis);
        $yAxis = svg.append('g').attr('class', 'y axis')
          .attr('transform', `translate(${margins.left})`)
          .call(yAxis);
    
        let rand = Math.floor(100000 * Math.random());
        var clip = svg.append("defs").append("svg:clipPath")
          .attr("id", "clip"+rand)
          .append("svg:rect")
          .attr("id", "clip-rect"+rand)
          .attr("x", margins.left+1)
          .attr("y", margins.top)
          .attr("width", width-margins.left - margins.right+1)
          .attr("height", height - margins.top - margins.bottom);
                            
        var visCont = svg.append('g')
                .attr("clip-path", "url(#clip" + rand + ")")
                .attr('class', 'vis');
    
        $data = visCont.append('path').attr('class', 'line data');
        $dataMin = visCont.append('path').attr('class', 'lineMin data');
        $dataMax = visCont.append('path').attr('class', 'lineMax data');
        $area = visCont.append('path').attr('class', 'area data');
        createToolTip();
    }
    
    const update = (pTimingData) => {
        if (pTimingData) {
            timingData = pTimingData;
        }

        if (!xScale || !timingData ) {
          return;
        }
        data = (timingData);
        let now = Date.now();
        minTime = now - duration - 1000;
        xScale.domain([now - duration, now]);
        let y = 100;
        if (data) {
          y = getHighestVisibleY();
        }
        yScale.domain([0, y]);
        $xAxis.call(xAxis);
        $yAxis.call(yAxis);
        if (data) {
          if (timingType == "LATENCY") {
            $area.datum(data).attr('d', area);
          }
          $data.datum(data).attr('d', line);
          $dataMax.datum(data).attr('d', lineMax);
          $dataMin.datum(data).attr('d', lineMin);
        }
      }
    
    const getHighestVisibleY = () => {
        if (timingType == "LATENCY") {
          let fn = getMinY;
          if (visibilities[MAX]) {
            fn = getMaxY;
          }
          else if (visibilities[AVG]) {
            fn = getY;
          }
          return (d3.max(data, d => fn.call(this, d)) || 0) + 1;
        }
        else {
          return (d3.max(data, d => getTotalTxns(d)) || 0) + 1;
        }
    }
      
    const validatePoint = (point) => {
        if (!point || !point.sampleTime) return false;
        if (minTime > 0) return point.sampleTime >= minTime;
        return true;
      }
    const getX = (point) => {
        return point ? point.sampleTime : 0;
      }
    
    const getHighestYFromPoint = (point) => {
        if (timingType == 'LATENCY') {
          let fn = getMinY;
          if (visibilities[MAX]) {
            fn = getMaxY;
          }
          else if (visibilities[AVG]) {
            fn = getY;
          }
          return fn.call(this, point);
        }
        else {
          let fn = getFailedTxns;
          if (visibilities[MAX]) {
            fn = getSuccessfulTxns;
          }
          else if (visibilities[AVG]) {
            fn = getTotalTxns;
          }
          return fn.call(this, point);
        }
    }
    const getMaxY = (point) => {
        if (validatePoint(point)) {
          return point.maxTimeUs / 1000.0;
        }
        return 0;
    }
    
    const getY = (point) => {
        return validatePoint(point) ? point.avgTimeUs/1000.0 : 0;
    }
    
    const getMinY = (point) => {
        return validatePoint(point) ? point.minTimeUs/1000.0 : 0;
    }
    
    const getTotalTxns = (point) => {
        return validatePoint(point) ? point.failedOps+point.successfulOps : 0;
    }
    
    const getSuccessfulTxns = (point) => {
        return validatePoint(point) ? point.successfulOps : 0;
    }
    
    const getFailedTxns = (point) => {
        return validatePoint(point) ? point.failedOps : 0;
    }
    
    const formatToOneDP = (num) =>  {
        return Math.round(num * 10) / 10;
    }

    const formatLatency = (point) => {
        return "Min:"+formatToOneDP(point.minTimeUs/1000.0) + ", Avg:" + formatToOneDP(point.avgTimeUs/1000.0) + ", Max:" + formatToOneDP(point.maxTimeUs/1000.0);
    }
    
    const formatThroughput = (point) => {
        return "Throughput:" + (point.successfulOps + point.failedOps) + " (" + point.successfulOps + ", "+point.failedOps +")";
    }
    
    const createToolTip = () => {
        $tooltip = svg.append('g')
            .attr('class', 'focus')
            .style('display', 'none');
    
        $tooltip.append('circle')
            .attr('r', 5);
    
        $tooltip.append('rect')
            .attr('class', 'tooltip')
            .attr('width', 200)
            .attr('height', 70)
            .attr('x', -210)
            .attr('y', -22)
            .attr('rx', 4)
            .attr('ry', 4);
    
        $tooltip.append('text')
            .attr('class', 'tooltip-date')
            .attr('x', -202)
            .attr('y', -2);
    
        // $tooltip.append('text')
        //     .attr('x', 18)
        //     .attr('y', 18)
        //     .text('Likes:');
    
        $tooltip.append('text')
            .attr('class', 'tooltip-latency')
            .attr('x', -202)
            .attr('y', 18);
    
        $tooltip.append('text')
            .attr('class', 'tooltip-throughput')
            .attr('x', -202)
            .attr('y', 38);
    
        svg.append('rect')
            .attr('class', 'overlay')
            .style('fill','none')
            .style('pointer-events', 'all')
            .attr('width', width)
            .attr('height', height)
            .on('mouseover', () => $tooltip.style('display', null))
            .on('mouseout', () => $tooltip.style('display', 'none'))
            .on('mousemove', (evt) => {
              let cursor = d3.pointer(evt)[0];
              let date = xScale.invert(cursor);
              let time = date.getTime();
              let point = d3.bisector(function(d) { 
                return d.sampleTime; 
              });
              let closest = point.left(data, time, 1);
              if (closest > 0 && closest < data.length) {
                let thisPoint = data[closest-1];
                let nextPoint = data[closest];
                let dataPoint = time - thisPoint.sampleTime > time - nextPoint.sampleTime ? nextPoint : thisPoint;
                if (dataPoint) {
                  $tooltip.attr('transform', 'translate(' + xScale(dataPoint.sampleTime) + ',' + yScale(getHighestYFromPoint(dataPoint)) + ')');
                  $tooltip.select('.tooltip-date').text(dateFormatter(new Date(dataPoint.sampleTime)));
                  $tooltip.select('.tooltip-latency').text(formatLatency(dataPoint));
                  $tooltip.select('.tooltip-throughput').text(formatThroughput(dataPoint));
                }
              }
            });
    }

    setVisibleDuration = (pDuration) => {
        if (duration && duration > 1000) {
            duration = pDuration;
            update();
        }
    }
    
    construct();

    return {
        changeTitle: changeTitle,
        update: update,
        setVisibleDuration : setVisibleDuration
    };
}