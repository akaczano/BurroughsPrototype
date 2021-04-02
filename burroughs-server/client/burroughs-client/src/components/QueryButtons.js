import React from 'react';
import { connect } from 'react-redux';
import { executeQuery, terminateQuery, setKeepTable, getStatus } from '../actions/queryActions';
import { cleanup } from '../actions/topicActions';

import { Row, Button, Spinner } from 'react-bootstrap'; 

const buttonStyle = {        
    marginTop: '3px',    
    marginRight: '5px',
    paddingTop: '1px',
    paddingBottom: '1px',    
};

class QueryButtons extends React.Component {    

    componentDidMount() {
        this.props.getStatus();
    }

    getExecButton() {
        if (this.props.executing) {
            return <Spinner animation="border" />;
        }
        else {
            return <>Execute</>
        }        
    }

    getTerminateButton() {
        if (this.props.terminating) {
            return <Spinner animation="border" />;
        }
        else {
            return <>Terminate</>
        }     
    }

    getCleanupButton() {
        if (this.props.cleaningUp) {
            return <Spinner animation="border" />;
        }
        else {
            return <>Clean Up</>;
        }
    }

    render() {
        return (
            <Row style={{margin: '0px', marginLeft: '5px'}}>
                <Button 
                    variant="dark" 
                    disabled={this.props.queryActive || (!this.props.tableName || this.props.tableName.length < 1)} 
                    style={{...buttonStyle }}
                    onClick={ () => this.props.executeQuery(this.props.code.trim()) }
                >
                    {this.getExecButton()}
                </Button>
                <Button 
                    variant="dark" 
                    disabled={!this.props.queryActive} 
                    style={buttonStyle}
                    onClick={ () => this.props.terminateQuery() }
                >
                    {this.getTerminateButton()}
                </Button>
                <p style={{marginTop: '6px', marginBottom: '0px'}}>
                    <input 
                        type="checkbox" 
                        style={{marginRight: '3px'}} 
                        checked={this.props.keepTable}  
                        onChange={() => this.props.setKeepTable()}
                    />
                    Keep Table
                </p>
                <Button variant="dark" 
                    style={{position: 'absolute', right: '15px', ...buttonStyle}}
                    disabled={this.props.queryActive}
                    onClick={e => {this.props.cleanup()}}
                >
                    {this.getCleanupButton()}
                </Button>
            </Row>
        );
    }
}

const mapStateToProps = state => {
    return {
        code: state.core.code,
        executing: state.query.queryExecuting,
        terminating: state.query.queryTerminating,
        queryActive: state.query.queryActive,
        tableName: state.core.dbInfo.table,
        keepTable: state.query.keepTable,
        cleaningUp: state.topic.cleaningUp
    };
};

export default connect(mapStateToProps, 
    { executeQuery, terminateQuery, setKeepTable, getStatus, cleanup })
(QueryButtons);
