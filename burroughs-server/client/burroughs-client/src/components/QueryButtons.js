import React from 'react';
import { connect } from 'react-redux';
import { executeQuery, terminateQuery, setKeepTable } from '../actions/basicActions';

import { Row, Button, Spinner } from 'react-bootstrap'; 

const buttonStyle = {        
    marginTop: '3px',    
    marginRight: '5px',
    paddingTop: '1px',
    paddingBottom: '1px',
    marginLeft: '3px'  
};

class QueryButtons extends React.Component {    

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

    render() {
        return (
            <Row>
                <Button 
                    variant="info" 
                    disabled={this.props.queryActive || this.props.tableName.length < 1} 
                    style={{...buttonStyle, marginLeft: '14px'}}
                    onClick={ () => this.props.executeQuery(this.props.code) }
                >
                    {this.getExecButton()}
                </Button>
                <Button 
                    variant="info" 
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
            </Row>
        );
    }
}

const mapStateToProps = state => {
    return {
        code: state.core.code,
        executing: state.core.queryExecuting,
        terminating: state.core.queryTerminating,
        queryActive: state.core.queryActive,
        tableName: state.core.dbInfo.table,
        keepTable: state.core.keepTable
    };
};

export default connect(mapStateToProps, { executeQuery, terminateQuery, setKeepTable })(QueryButtons);
