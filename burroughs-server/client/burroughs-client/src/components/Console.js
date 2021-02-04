import React from 'react';
import { connect } from 'react-redux';

import { loadMessages } from '../actions/basicActions';

class Console extends React.Component {        
    
    displayMessage(message) {          
        const color = message.color === 'NONE' ? 'black' : message.color.toLowerCase();
        const { time, text } = message;
        return ( 
            <p style={{margin: '3px', color }} key={time + text}>
                {(new Date(time)).toLocaleTimeString()}:  {text}
            </p>
        );
    }

    render() {
        return(
            <div>
                {this.props.messages.map(this.displayMessage)}
            </div>
        );
    }
}

const mapStateToProps = state => {
    return {
        messages: state.core.consoleMessages
    };
};

export default connect(mapStateToProps, { loadMessages })(Console);
