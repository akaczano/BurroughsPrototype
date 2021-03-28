import React from 'react';
import { Row } from 'react-bootstrap';
import { connect } from 'react-redux';

import CodeEditor from './CodeEditor';
import TopicList from './TopicList';
import Header from './Header';
import DatabaseInfo from './DatabaseInfo';
import OutputTabs from './OutputTabs';
import SchemaDisplay from './SchemaDisplay';
import Gutter from './Gutter';
import ToolBar from './ToolBar';

import './App.css';
import HorizontalGutter from './HorizontalGutter';


const colStyle = {
    margin: '0px',
    padding: '0px',    
    overflow: 'hidden'
}

class App extends React.Component {
    render() {
        return (
            <>
                <SchemaDisplay />
                <div style={{ margin: '0px', width: '100%' }}>
                    <Header />
                    <Row style={{ height: '0.8vh', width: '100%', margin: '0px' }} className="accent-border-header"></Row>
                    <Row style={{marginRight: '0px', height: '89.2vh'}}>
                        <div style={{ ...colStyle, width: this.props.columnSizes[0] + '%' }}>
                            <TopicList />
                            <DatabaseInfo />
                        </div>
                        <Gutter style={{ ...colStyle, width: '0.5%' }} />
                        <div style={{ ...colStyle, width: this.props.columnSizes[1] -0.5 + '%' }}>
                            <ToolBar />
                            <CodeEditor style={{ margin: '0px' }} />
                            <HorizontalGutter />                                                   
                            <OutputTabs style={{ margin: '0px' }} />
                        </div>
                    </Row>
                </div>
            </>
        );
    }

}

const mapStateToProps = state => {
    return {
        columnSizes: state.ui.columnSizes
    };
};

export default connect(mapStateToProps)(App);