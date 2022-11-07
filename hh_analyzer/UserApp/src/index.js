import React, { Component } from 'react';
import ReactDOM from 'react-dom';
import { Layout } from 'antd';
import './dashboard.css';
import BarChart from './charts/barchart'


const { Sider, Content} = Layout;
const { Kafka } = require("kafkajs")

const extractor_topic = process.env.EXTRACTOR_KAFKA_THEME

const kafka = new Kafka({ clientId: "my_app", brokers: ["kafka1:9092"] })
const producer = kafka.producer()

const call_extractor = async (start_date, end_date) => {
	await producer.connect()
    try {
        const message = String(start_date) + " " + String(end_date)
        await producer.send({
            extractor_topic,
            messages: [
                {
                    key: "0",
                    value: message,
                },
            ],
        })
        console.log("writes: ", message)
    } catch (err) {
        console.error("could not write message " + err)
    }
}

class OptionsView extends Component {
    render() {
        let {startDate, endDate} = this.props;

        return (
            <div id='options' className='pane'>
                <div className='header'>Choose the range of interest</div>
                <div>
                    <script src="https://cdnjs.cloudflare.com/ajax/libs/moment.js/2.22.2/moment.min.js"></script>
                    <script src="lightpick.js"></script>
                    <script src="./datepick.js"></script>
                </div>
            </div>
        )
    }
}

class MainView extends Component {
    render() {
        let bar_data = [15, 20, 5, 10, 22, 14, 3]
        let bar_labels = ['red', 'comm', 'frog', 'ral', 'gnome', 'fall', 'kli']
        const data = []
        for (let i = 0; i < bar_data.length; i++) {
            const d = {
                name: bar_labels[i],
                marks: bar_data[i],
            }
            data.push(d)
        }
        console.log(data)
        return (
            <div id='dashboardView' className='pane'>
                <div className='header'>Test data</div>
                <div style={{ overflowX: 'scroll',overflowY:'hidden' }}>
                <BarChart data={data} width={900} height={500}/>
                </div>
            </div>
        )
    }
}

class Dashboard extends Component {
    constructor(props) {
        super(props);
        this.state = {
            startDate: Date.now() - 30,
            endDate: Date.now(),
        }
    }

    changeStartDate = value => {
        this.setState({
            startDate: value
        })
    }

    changeEndDate = value => {
        this.setState({
            endDate: value
        })
    }

    render() {
        const {startDate, endDate} = this.state;
        return (
            <div>
                <Layout style={{ height: 300 }}>
                    <Sider width={300} style={{backgroundColor:'#eee'}}>
                        <Content style={{ height: 300 }}>
                            <OptionsView startDate={startDate} endDate={endDate}/>
                        </Content>
                    </Sider>
                    <Layout>
                        <Content style={{ height: 300 }}>
                            <MainView/>
                        </Content>
                    </Layout>
                </Layout>
            </div>
        )
    }
}

ReactDOM.render(<Dashboard />, document.getElementById('root'));