import React, { Component } from 'react';
import * as d3 from 'd3';

export default class BarChart extends Component {

    drawBar(props) {
        d3.select('.vis-barchart > *').remove();
        const margin = {top: 20, right: 20, bottom: 30, left: 40};
        const width = props.width - margin.left - margin.right;
        const height = props.height - margin.top - margin.bottom;

        let svg = d3.select('.vis-barchart').append('svg')
                .attr('width',width + margin.left + margin.right)
                .attr('height',height + margin.top + margin.bottom)
                .append("g")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        // Scale the range of the data in the domains
        let x = d3.scaleBand()
              .range([0, width])
              .padding(0.1);
        let y = d3.scaleLinear()
              .range([height, 0]);


        x.domain(props.data.map(function (d) {
            return d.name
        }));
        y.domain([0, d3.max(props.data, function (d) {
            return d.marks;
        }), ]);

        // append the rectangles for the bar chart
        svg.selectAll(".bar")
            .data(props.data)
            .enter().append("rect")
            .attr("class", "bar")
            .attr("x", function (d) {
                return x(d.name);
            })
            .attr("width", x.bandwidth())
            .attr("y", function (d) {
                return y(d.marks);
            })
            .attr("height", function(d) { return height - y(d.marks); });

        // add the x Axis
        svg.append("g")
            .attr("transform", "translate(0," + height + ")")
            .call(d3.axisBottom(x));

        // add the y Axis
        svg.append("g")
            .call(d3.axisLeft(y));
    }

    componentDidMount() {
        this.drawBar(this.props);
    }

    componentDidUpdate(preProps) {
        this.drawBar(this.props);
    }

    render() {
        return (
            <div className='vis-barchart'/>
        )
    }
}