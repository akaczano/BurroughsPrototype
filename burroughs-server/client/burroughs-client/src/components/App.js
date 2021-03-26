import React from 'react';
import { Row, Col } from 'react-bootstrap';
import { Provider } from 'react-redux';

import store from '../store';
import CodeEditor from './CodeEditor';
import QueryButtons from './QueryButtons';
import TopicList from './TopicList';
import Header from './Header';
import DatabaseInfo from './DatabaseInfo';
import OutputTabs from './OutputTabs';
import SchemaDisplay from './SchemaDisplay';

import './App.css';


const colStyle = {
    margin: '0px',
    padding: '0px',
    paddingBottom: '100%',
    marginBottom: '-100%',
    overflow: 'hidden'
}

class App extends React.Component {
    render() {
        return (
            <Provider store={store}>
                <SchemaDisplay />
                <div style={{ margin: '0px' }}>
                    <Header />
                    <Row>
                        <Col md={3} style={{ ...colStyle, borderRight: '4px solid #7d1127' }}>
                            <TopicList/>
                            <DatabaseInfo />
                        </Col>
                        <Col md={9} style={colStyle}>
                            <CodeEditor style={{margin: '0px'}}/>
                            <QueryButtons style={{margin: '10px'}} />
                            <OutputTabs style={{margin: '0px'}} />
                        </Col>
                    </Row>
                </div>
            </Provider >
        );
    }

}

export default App;