import React from 'react';
import { Row } from 'react-bootstrap';


import Header from './Header';
import SplitLayout from './SplitLayout';

import SchemaDisplay from './SchemaDisplay';

import './App.css';


class App extends React.Component {
    render() {
        return (
            <>
                <SchemaDisplay />
                <div style={{ margin: '0px', width: '100%' }}>
                    <Header />
                    <Row style={{ height: '0.8vh', width: '100%', margin: '0px' }} className="accent-border-header"></Row>
                    <Row style={{marginRight: '0px', height: '89.2vh'}}>
                        <SplitLayout />
                    </Row>
                </div>
            </>
        );
    }

}

export default App;