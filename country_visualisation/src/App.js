
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
import { interpolateOranges } from "d3-scale-chromatic"
import { scaleLinear, scaleSequential } from "d3-scale"
import { geoAzimuthalEqualArea } from "d3-geo"
import Papa from "papaparse"
import world_50m from "./world-50m.json"
import "./App.css";

const countryScale = scaleLinear().domain([1, 3524]).range([0.5,5])
const connScale = scaleLinear().domain([1, 8000])
const opacityScale = scaleLinear().domain([0,164480]).range([0.02,1])
const nodeColor = scaleSequential().domain([0,1]).interpolator(interpolateOranges)

const countries = [{"country":"Austria","lat":47.585810005,"lng":14.137076948},{"country":"Belgium","lat":50.640682937,"lng":4.661070427},{"country":"Bulgaria","lat":42.755008391,"lng":25.23641224},{"country":"Croatia","lat":45.118679585,"lng":15.435623834},{"country":"Cyprus","lat":35.0501743,"lng":33.226229724},{"country":"Czech Republic","lat":49.738854028,"lng":15.331770138},{"country":"Denmark","lat":55.959300779,"lng":10.053934054},{"country":"Estonia","lat":58.672040787,"lng":25.477224001},{"country":"Finland","lat":64.522512801,"lng":26.158834376},{"country":"France","lat":46.559417044,"lng":2.550539953},{"country":"Germany","lat":51.110631049,"lng":10.392277932},{"country":"Greece","lat":39.0,"lng":22.0},{"country":"Hungary","lat":47.16708877,"lng":19.4245317},{"country":"Ireland","lat":53.175879846,"lng":-8.146006147},{"country":"Italy","lat":42.83333,"lng":12.83333},{"country":"Latvia","lat":56.85460987,"lng":24.926839006},{"country":"Lithuania","lat":55.336936081,"lng":23.900396649},{"country":"Luxembourg","lat":49.776828429,"lng":6.092325247},{"country":"Malta","lat":35.922548856,"lng":14.40007259},{"country":"Netherlands","lat":52.249375293,"lng":5.616126398},{"country":"Poland","lat":52.123790154,"lng":19.398768986},{"country":"Portugal","lat":39.593139046,"lng":-8.51981299},{"country":"Romania","lat":45.845854975,"lng":24.973472215},{"country":"Slovakia","lat":48.708702075,"lng":19.48774911},{"country":"Slovenia","lat":46.119554805,"lng":14.821760495},{"country":"Spain","lat":40.22794966,"lng":-3.646063105},{"country":"United Kingdom","lat":53.40838660500003,"lng":-1.9695595439999352}]

const wrapperStyles = {
  width: "100%",
  maxWidth: 980,
  margin: "0 auto",
}

class App extends Component {

  state = {
    nodes: [],
    markers: [],
    edges: [],
    conns: [],
    zoom: 7,
    year: 2007
  }

  constructor() {
    super()

    this.handleZoomIn = this.handleZoomIn.bind(this)
    this.handleZoomOut = this.handleZoomOut.bind(this)
    this.updateMarkers = this.updateMarkers.bind(this)
    this.handleYear = this.handleYear.bind(this)
    this.updateConns = this.updateConns.bind(this)
    this.buildCurves = this.buildCurves.bind(this)
  }

  updateMarkers() {
    const markers = this.state.nodes.filter(i => i['Year'] === this.state.year.toString()).map((item) => {
      const coords = countries.filter(c => c['country'] === item['Name'])
      if(coords[0]) {
        const connectivity = ((item['Edges']*item['Connectivity'])/item['Nodes']) + 0.2
        
        return { 
          markerOffset: -15,
          name: item['Name'],
          coordinates: [coords[0]['lng'],coords[0]['lat']],
          width: countryScale(item['Nodes']),
          color: nodeColor(connectivity)
        }
      } else {
        return { markerOffset: 35, name: item['Name'], coordinates: [0,0], width: 0, color: '#fff' }
      }
    });

    this.setState({
      markers
    })
  }

  updateConns() {
    const conns = this.state.edges
      .filter(i => i['Year'] === this.state.year.toString())
      .filter(item => {
        const source = item['Source'].split('-')[0], sourceCoords = countries.filter(c => c['country'] === source);
        const target = item['Target'].split('-')[0], targetCoords = countries.filter(c => c['country'] === target);
        if (sourceCoords[0] && targetCoords[0]) {
          return true
        } else {
          return false
        }
      })
      .map((item) => {
        const source = item['Source'].split('-')[0], sourceCoords = countries.filter(c => c['country'] === source);
        const target = item['Target'].split('-')[0], targetCoords = countries.filter(c => c['country'] === target);
        const weight = parseInt(item['Edges'], 10);
        
        let connectivity = 0;
        if(item['Percent']) {
          connectivity = parseFloat(item['Percent'].replace('\r',''), 10);
        }

        return {
          coordinates: {
            start: [sourceCoords[0]['lng'],sourceCoords[0]['lat']],
            end: [targetCoords[0]['lng'],targetCoords[0]['lat']],
          },
          width: connScale(weight),
          opacity: opacityScale(connectivity*weight)
        }
      });

    this.setState({
      conns
    })
  }

  componentWillMount() {
    Papa.parse('nodes.csv', {
      header: true,
      delimiter: ',',
      download: true,
      complete: (result) => {
        this.setState({
          nodes: result.data
        });

        this.updateMarkers();
      }
    });

    Papa.parse('edges.csv', {
      header: true,
      delimiter: ',',
      download: true,
      complete: (result) => {
        this.setState({
          edges: result.data
        });

        this.updateConns();
      }
    });
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
    this.setState({ year: e.target.value})
    this.updateMarkers()
    this.updateConns()
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

  render() {
    const { markers, conns } = this.state;

    return (
      <div style={wrapperStyles}>
        <button onClick={ this.handleZoomIn }>{ "Zoom in" }</button>
        <button onClick={ this.handleZoomOut }>{ "Zoom out" }</button>
        
        Year: ({this.state.year}): <input
          type="range"
          min="2007"
          max="2017"
          value={this.state.year}
          step="1"
          onChange={this.handleYear}
        />
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
              <Annotation 
                dx={ -150 } 
                dy={ -15 } 
                subject={  [2.550539953, 46.559417044] } 
                preserveMarkerAspect={false}
                strokeWidth={ 0.2 } 
                stroke="#607D8B">
                <text x={-0.5} y={0.5} style={{fontSize: 2.5}}>
                  { "France" }
                </text>
              </Annotation>
              <Annotation dx={ -10 } dy={ 55 } subject={  [12.83333,42.83333] } strokeWidth={ 0.2 } stroke="#607D8B">
                <text x={2} y={2} style={{fontSize: 2.5}}>
                  { "Italy" }
                </text>
              </Annotation>
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
      </div>
    )
  }
}

export default App
