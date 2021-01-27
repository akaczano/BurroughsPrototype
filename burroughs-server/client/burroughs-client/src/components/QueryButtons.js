import React from 'react';
import { connect } from 'react-redux';
import { executeQuery, terminateQuery } from '../actions/basicActions';

import { Row, Button, Spinner } from 'react-bootstrap'; 

const buttonStyle = {        
    marginTop: '3px',    
    marginRight: '8px'  
};

class QueryButtons extends React.Component {
    render() {
        return (
            <Row>
                <Button variant="info" disabled={this.props.queryActive} style={{...buttonStyle, marginLeft: '14px'}}>
                    Execute
                </Button>
                <Button variant="info" disabled={!this.props.queryActive} style={buttonStyle}>
                    Terminate
                </Button>
            </Row>
        );
    }
}

const mapStateToProps = state => {
    return {
        executing: state.core.queryExecuting,
        terminating: state.core.queryTerminating,
        queryActive: state.core.queryActive
    };
};

export default connect(mapStateToProps, { executeQuery, terminateQuery })(QueryButtons);
