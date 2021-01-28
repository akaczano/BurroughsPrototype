import React from 'react';
import { connect } from 'react-redux';
import { Row } from 'react-bootstrap';

import { getConnection } from '../actions/basicActions';

class Header extends React.Component {

    componentDidMount() {
        this.props.getConnection();
        setInterval(this.props.getConnection, 5000);
    }

    getConnectionLabel() {
        if (!this.props.connection) {
            return <span style={{color: 'lightgray'}}>Getting status...</span>
        }
        else {            
            let connected = this.props.connection.ksqlConnected &&
                this.props.connection.dbConnected;
            if (connected) {
                return <span style={{color: '#2ad16d'}}>Connected</span>
            }
            else {
                return <span style={{color: 'red'}}>Disconnected</span>
            }
        }
    }

    render() {
        return (            
            <Row style={{ padding: '7px', margin: '0px', backgroundColor: '#474a48'}}>                
                <Row style={{position: 'absolute', right: '0px', marginRight: '20px', color: 'white'}}>
                    <span>Status: {this.getConnectionLabel()}</span>                    
                </Row>  
                <h4 style={{color: 'white', float: 'left'}}>Burroughs</h4> 
            </Row>
        );
    }
}

const mapStateToProps = state => {
    return {
        connection: state.core.connection
    };
};

export default connect(mapStateToProps, { getConnection })(Header);