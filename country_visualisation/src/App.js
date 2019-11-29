
import React, { Component } from "react"
import {
  ComposableMap,
  ZoomableGroup,
  Geographies,
  Geography,
  Markers,
  Marker,
  Annotations,
  Annotation,
  Lines,
  Line
} from "react-simple-maps"
import {
  Motion, spring
} from "react-motion"
import { interpolateOranges, interpolateOrRd } from "d3-scale-chromatic"
import { scaleLinear, scaleSequential } from "d3-scale"
import { geoAzimuthalEqualArea } from "d3-geo"
import { csv } from "d3-fetch"
import world_50m from "./world-50m.json"
import "./App.css"
import _ from "lodash"

const countries = require('./countries_details.json').map(item => ({
  name: item.name,
  latlng: item.latlng
}));

const wrapperStyles = {
  width: "100%",
  maxWidth: 980,
  margin: "0 auto",
}

const computeEdgeDetails = (edges, nodes) => {
  return edges.filter(edge => {
      const snode = nodes.filter(i => i['Id'] === edge['SourceId'])[0]
      const tnode = nodes.filter(i => i['Id'] === edge['TargetId'])[0]

      if (snode && tnode) return edge;
      else return false
    }).map(edge => {
    const snode = nodes.filter(i => i['Id'] === edge['SourceId'])[0]
    const tnode = nodes.filter(i => i['Id'] === edge['TargetId'])[0]

    const totalNodes = parseInt(snode['Nodes']) + parseInt(tnode['Nodes'])
    const totalEdges = parseInt(snode['Edges']) + parseInt(tnode['Edges'])
    const connectivity = (totalEdges / ((totalNodes*totalNodes-1)/2))

    return {
      ...edge,
      Year: parseInt(edge['Year']),
      totalNodes,
      totalEdges,
      connectivity
    }
  });
}

const computeNodesDetails = (nodes, edges) => {
  const countriesPerYear = _.groupBy(edges, 'Year');

  return nodes.map(node => {
    const countNodes = parseInt(node['Nodes'])
    const countEdges = parseInt(node['Edges'])
    const connectivity = (countEdges / ((countNodes*(countNodes-1))/2))

    const sedges = _.filter(countriesPerYear[node['Year']], { Sname: node['Name']}).length
    const tedges = _.filter(countriesPerYear[node['Year']], { Tname: node['Name']}).length

    return {
      ...node,
      Year: parseInt(node['Year']),
      Nodes: countNodes,
      Edges: countEdges,
      inConnectivity: connectivity,
      outConnectivity: sedges+tedges
    }
  });
}

class App extends Component {

  state = {
    subject: 'PHYS',
    year: 2007,
    nodes: [],
    edges: [],
    markers: [],
    conns: [],
    zoom: 7,
    playYears: false
  }

  constructor() {
    super()

    this.handleZoomIn = this.handleZoomIn.bind(this)
    this.handleZoomOut = this.handleZoomOut.bind(this)
    this.updateMarkers = this.updateMarkers.bind(this)
    this.handleYear = this.handleYear.bind(this)
    this.updateConns = this.updateConns.bind(this)
    this.buildCurves = this.buildCurves.bind(this)
    this.startPlay = this.startPlay.bind(this)

    this.playInterval = null;

    this.loadData('PHYS');
  }

  changeSubject = (e) => {
    this.loadData(e.target.value);
  }

  loadData = (subject) => {
    const data = Promise.all([
      csv(`/data/edgelist_tari_${subject}.csv`),
      csv(`/data/nodelist_tari_${subject}.csv`)
    ]).then(([edges, nodes]) => {
      this.setState({
        edges: computeEdgeDetails(edges, nodes),
        nodes: computeNodesDetails(nodes, edges)
      }, () => {
        this.updateScales()
      })
    })
  }

  updateMarkers() {
    const { nodes, year } = this.state;
    const markers = nodes.filter(i => i['Year'] === year).map((item) => {
      const coords = countries.filter(c => c['name'] === item['Name'])[0];
        // const connectivity = ((item['Edges']*item['Connectivity'])/item['Nodes']) + 0.2
      if (item['Name'] == 'Germany')
        console.log(this.countryScale(item['Nodes']))

      return { 
        markerOffset: -15,
        name: item['Name'],
        coordinates: [coords['latlng'][1],coords['latlng'][0]],
        width: this.countryScale(item['Nodes']),
        color: this.nodeColor(item['outConnectivity'])
      }
    });

    this.setState({
      markers
    })
  }

  updateConns() {
    const { edges, year } = this.state;

    const conns = edges.filter(i => i['Year'] === year)
      .map((item) => {
        const sourceCoords = countries.filter(c => c['name'] === item['Sname'])[0]
        const targetCoords = countries.filter(c => c['name'] === item['Tname'])[0]
        const weight = parseInt(item['Edges'], 10);
        const connectivity = item['connectivity'];

        return {
          coordinates: {
            start: [sourceCoords['latlng'][1],sourceCoords['latlng'][0]],
            end: [targetCoords['latlng'][1],targetCoords['latlng'][0]],
          },
          width: this.connScale(weight),
          opacity: this.opacityScale(weight)
        }
      });

    this.setState({
      conns
    })
  }

  updateScales = () => {
    const { nodes, edges } = this.state;
    const [{Nodes: minNodes}, {Nodes: maxNodes}] = [_.minBy(nodes, 'Nodes'), _.maxBy(nodes, 'Nodes')]
    const [{Edges: minEdges}, {Edges: maxEdges}] = [_.minBy(edges, 'Edges'), _.maxBy(edges, 'Edges')]
    const [{outConnectivity: minOutConn}, {outConnectivity:maxOutConn}] = [_.minBy(nodes, 'outConnectivity'), _.maxBy(nodes, 'outConnectivity')]

    this.countryScale = scaleLinear().domain([minNodes, maxNodes]).range([0.5,6])
    this.connScale = scaleLinear().domain([minEdges, maxEdges])
    this.opacityScale = scaleLinear().domain([minEdges, maxEdges]).range([0.02,1])
    this.nodeColor = scaleSequential().domain([minOutConn,maxOutConn]).interpolator(interpolateOranges)

    this.updateMarkers()
    this.updateConns()
  }


  handleZoomIn() {
    this.setState({
      zoom: this.state.zoom + 0.5,
    })
  }

  handleZoomOut() {
    this.setState({
      zoom: this.state.zoom - 0.5,
    })
  }

  handleYear(e) {
    this.setState({ year: parseInt(e.target.value) }, () => {
      this.updateMarkers()
      this.updateConns()
    });
  }

  startPlay() {
    this.setState({ playYears: true });

    this.playInterval = setInterval(() => {
      const currentYear = this.state.year;
      const nextYear = (currentYear >= 2017) ? 2007 : currentYear+1;

      this.setState({ year: nextYear })
      this.updateMarkers()
      this.updateConns()
    }, 1000);
  }

  stopPlay = () => {
    this.setState({ playYears: false });
    clearInterval(this.playInterval)
  }

  projection(width, height, config) {
    return geoAzimuthalEqualArea().scale(config.scale)
  }

  buildCurves(start, end, arc) {
    const x0 = start[0];
    const x1 = end[0];
    const y0 = start[1];
    const y1 = end[1];
    const curve = {
      forceUp: `${x1} ${y0}`,
      forceDown: `${x0} ${y1}`
    }[arc.curveStyle];

    return `M ${start.join(' ')} Q ${curve} ${end.join(' ')}`;
  }

  generatePlot = (year) => {
    const { nodes, edges } = this.state;
    const markers = nodes.filter(i => i['Year'] === year).map((item) => {
      console.log(item);
      const coords = countries.filter(c => c['name'] === item['Name'])[0];
      
      if (item['Name'] == 'Germany')
        console.log(this.countryScale(item['Nodes']))

      return { 
        markerOffset: -15,
        name: item['Name'],
        coordinates: [coords['latlng'][1],coords['latlng'][0]],
        width: this.countryScale(item['Nodes']),
        color: this.nodeColor(item['outConnectivity'])
      }
    });
  
    const conns = edges.filter(i => i['Year'] === year)
      .map((item) => {
        const sourceCoords = countries.filter(c => c['name'] === item['Sname'])[0]
        const targetCoords = countries.filter(c => c['name'] === item['Tname'])[0]
        const weight = parseInt(item['Edges'], 10);
        const connectivity = item['connectivity'];

        return {
          coordinates: {
            start: [sourceCoords['latlng'][1],sourceCoords['latlng'][0]],
            end: [targetCoords['latlng'][1],targetCoords['latlng'][0]],
          },
          width: this.connScale(weight),
          opacity: this.opacityScale(weight)
        }
      });
    
    return { markers, conns }
  }

  renderYear = (year) => {
    if (!this.countryScale) return null
    const { markers, conns } = this.generatePlot(year);

    return (
      <ComposableMap
        projectionConfig={this.projection}
        width={1000}
        height={700}
        style={{
          width: "100%",
          height: "auto",
        }}
        >
        <ZoomableGroup center={[10.08, 52.57]} zoom={this.state.zoom}>
          <Geographies geography={world_50m}>
            {(geographies, projection) =>
              geographies.map((geography, i) =>
                  <Geography
                    key={i}
                    geography={geography}
                    projection={projection}
                    style={{
                      default: {
                        fill: "#ECEFF1",
                        stroke: "#607D8B",
                        strokeWidth: 0.1,
                        outline: "none",
                      },
                      hover: {
                        fill: "#ECEFF1",
                        stroke: "#607D8B",
                        strokeWidth: 0.1,
                        outline: "none",
                      },
                      pressed: {
                        fill: "#ECEFF1",
                        stroke: "#607D8B",
                        strokeWidth: 0.1,
                        outline: "none",
                      },
                    }}
                  />
                
              )
            }
          </Geographies>
          
          <Markers>
            {markers.map((marker, i) => (
              <Marker
                key={i}
                marker={marker}
                preserveMarkerAspect={false}
                >
                  <circle
                    cx={0}
                    cy={0}
                    r={marker.width}
                    style={{
                      stroke: "#FF5722",
                      strokeWidth: 0,
                      fill: marker.color,
                    }}
                  />
              </Marker>
            ))}
          </Markers>
          <Annotations>
            
          </Annotations>
          <Lines>
            {conns.map((item, i) => (
              <Line
                key={i}
                line={{
                  coordinates: item.coordinates
                }}
                strokeWidth={0.2}
                preserveMarkerAspect={false}
                style={{
                  default: { stroke: "#666", opacity: item.opacity },
                  hover:   { stroke: "#666", opacity: item.opacity  },
                  pressed: { stroke: "#666", opacity: item.opacity  },
                }}
              />
            ))}
            
          </Lines>
        </ZoomableGroup>
      </ComposableMap>
    )
  }

  render() {
    const { markers, conns, playYears } = this.state;

    return (
      <div style={wrapperStyles}>
        <select onChange={ this.changeSubject }>
          <option value='PHYS' selected>PHYS</option>
          <option value='SOCI'>SOCI</option>
          <option value='MEDI'>MEDI</option>
        </select>
        <button onClick={ this.handleZoomIn }>{ "Zoom in" }</button>
        <button onClick={ this.handleZoomOut }>{ "Zoom out" }</button>
        {playYears 
          ? <button onClick={ this.stopPlay }>Stop</button>
          : <button onClick={ this.startPlay }>Start timeframe</button>
        }
        
        Year: ({this.state.year}): <input
          type="range"
          min="2007"
          max="2017"
          value={this.state.year}
          step="1"
          onChange={this.handleYear}
        />
        {_.range(2007, 2018).map((year) =>
          this.renderYear(year)
        )}

      </div>
    )
  }
}

export default App
