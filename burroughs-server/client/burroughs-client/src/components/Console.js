import React from 'react';
import { connect } from 'react-redux';
import { Form, Table, Row, Col } from 'react-bootstrap';

import { loadMessages } from '../actions/basicActions';

class Console extends React.Component {
    state = {debugLevel: 4}

    displayMessage(message) {
        //const color = message.color === 'NONE' ? 'black' : message.color.toLowerCase();
        const { time, text } = message;
        return (
            <tr key={time + text}>
                <td>{(new Date(time)).toLocaleTimeString()}</td>
                <td>{text}</td>
            </tr>
        );
    }

    render() {
        return (
            <>                
                <Form style={{width: '100%', position: 'sticky', top: 0, backgroundColor: '#e6e6e6'}}>
                    <Form.Group as={Row} style={{padding: '3px', margin: '0px', width: '100%'}}>
                        <Col>
                        <Form.Label><strong>Debug Level</strong> </Form.Label>
                        </Col>
                        <Col>
                        <Form.Control as="select" style={{maxHeight: '35px'}}
                            value={this.state.debugLevel}
                            onChange={e => this.setState({debugLevel: e.target.value})}
                        >
                            <option value="4">Default</option>
                            <option value="5">Level 1</option>
                            <option value="6">Level 2</option>
                        </Form.Control>
                        </Col>
                    </Form.Group>                    
                </Form>
                <Table className="output-messages" bordered>
                    <thead>
                        <tr>
                            <th>Time</th>
                            <th>Message</th>
                        </tr>
                    </thead>
                    <tbody>
                        {this.props.messages.filter(m => m.debugLevel == this.state.debugLevel)
                        .map(this.displayMessage)}
                    </tbody>
                </Table>
            </>
        );
    }
}

const mapStateToProps = state => {
    return {
        messages: state.core.consoleMessages
    };
};

export default connect(mapStateToProps, { loadMessages })(Console);
